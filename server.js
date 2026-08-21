// USCCS Schedule Change Monitor
//
// Polls SportsEngine's full game schedule on a schedule (default 3x/day)
// and records any game whose `updated` timestamp is newer than our last
// successful poll AND meaningfully later than its own `created` timestamp
// (distinguishing a genuine edit to an existing game from a brand-new game
// simply being added - both have a "recent update", only one is a real
// change worth reporting).
//
// State is deliberately minimal: just ONE timestamp (when we last
// successfully polled) plus a durable, growing log of every individual
// change ever detected (date/time/location/home/away). Each change is
// recorded exactly once, ever - a UNIQUE constraint + ON CONFLICT DO
// NOTHING guarantees the same change can never be inserted twice, even if
// a poll window is ever reprocessed.
//
// REQUIRED ENVIRONMENT VARIABLES:
//   SE_CLIENT_ID, SE_CLIENT_SECRET, SE_REFRESH_TOKEN, SE_ORG_ID - same as the other apps
//   DATABASE_URL              - the schedule monitor's OWN Supabase Postgres instance
//   POLL_INTERVAL_HOURS       - (optional) defaults to 8 (3x/day)
//   SEASON_END_DATE           - (optional but preferred) e.g. "2026-11-15" - polls exactly
//                                through the real season end, never drifting past it or
//                                falling short. Takes priority over POLL_WINDOW_DAYS if set.
//   POLL_WINDOW_DAYS          - (optional) fallback only, used if SEASON_END_DATE isn't set - defaults to 100
//   NEW_GAME_THRESHOLD_MINUTES - (optional) defaults to 5 - gap between created/updated
//                                 below which a game is treated as "new", not "edited"
//   PORT                      - usually set automatically by the host

const http = require('http');
const fs = require('fs');
const path = require('path');
const https = require('https');
const { Pool } = require('pg');

const PORT = process.env.PORT || 8787;
const HTML_FILE = path.join(__dirname, 'index.html');
const SE_CLIENT_ID = process.env.SE_CLIENT_ID;
const SE_CLIENT_SECRET = process.env.SE_CLIENT_SECRET;
const SE_REFRESH_TOKEN = process.env.SE_REFRESH_TOKEN;
const SE_ORG_ID = parseInt(process.env.SE_ORG_ID, 10);
const POLL_INTERVAL_HOURS = parseFloat(process.env.POLL_INTERVAL_HOURS || '8');
const POLL_WINDOW_DAYS = parseInt(process.env.POLL_WINDOW_DAYS || '100', 10); // fallback only - covers this season's Nov 15, 2026 end (87 days out as of Aug 20) with real margin
const SEASON_END_DATE = process.env.SEASON_END_DATE || null; // e.g. "2026-11-15" - preferred over POLL_WINDOW_DAYS

// ---------- Postgres ----------

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

pool.on('error', (err) => {
  console.error('[postgres] Unexpected error on idle client:', err.message);
});

// ---------- SportsEngine OAuth (same pattern as the other apps) ----------

let tokenCache = { accessToken: null, expiresAt: 0 };

function refreshAccessToken() {
  return new Promise((resolve, reject) => {
    if (!SE_CLIENT_ID || !SE_CLIENT_SECRET || !SE_REFRESH_TOKEN) {
      return reject(new Error('Missing SE_CLIENT_ID / SE_CLIENT_SECRET / SE_REFRESH_TOKEN environment variables.'));
    }
    const body = JSON.stringify({
      client_id: SE_CLIENT_ID,
      client_secret: SE_CLIENT_SECRET,
      refresh_token: SE_REFRESH_TOKEN,
      grant_type: 'refresh_token',
    });
    const req = https.request(
      {
        hostname: 'user.sportsengine.com',
        path: '/oauth/token',
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
      },
      (res) => {
        let data = '';
        res.on('data', (chunk) => (data += chunk));
        res.on('end', () => {
          try {
            const json = JSON.parse(data);
            if (!json.access_token) return reject(new Error('Token refresh failed: ' + data));
            tokenCache.accessToken = json.access_token;
            tokenCache.expiresAt = Date.now() + (json.expires_in || 1800) * 1000 - 60000;
            resolve(tokenCache.accessToken);
          } catch (e) {
            reject(new Error('Could not parse token response: ' + data));
          }
        });
      }
    );
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function getValidAccessToken() {
  if (tokenCache.accessToken && Date.now() < tokenCache.expiresAt) return tokenCache.accessToken;
  return refreshAccessToken();
}

async function callGraphQL(query, variables) {
  const token = await getValidAccessToken();
  const body = JSON.stringify({ query, variables });
  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        hostname: 'api.sportsengine.com',
        path: '/graphql',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
          Authorization: 'Bearer ' + token,
        },
      },
      (res) => {
        let data = '';
        res.on('data', (chunk) => (data += chunk));
        res.on('end', () => {
          try {
            const json = JSON.parse(data);
            if (json.errors) return reject(new Error('GraphQL error: ' + JSON.stringify(json.errors)));
            resolve(json.data);
          } catch (e) {
            reject(new Error('Non-JSON response from SportsEngine: ' + data.slice(0, 300)));
          }
        });
      }
    );
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// ---------- Fetching the full schedule (paginated) ----------

const EVENTS_QUERY = `
  query Events($orgId: Int!, $from: UTCDateTime!, $to: UTCDateTime!, $page: Int!, $perPage: Int!) {
    events(organizationId: $orgId, from: $from, to: $to, calendarEventType: GAME, page: $page, perPage: $perPage) {
      results {
        id
        eventTeams { name team { program { primaryName } divisionId } homeTeam }
        start
        subvenue { name venueId venueName }
        subvenueId
        updated
        created
      }
      pageInformation { count page pages }
    }
  }`;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Retries a single page fetch on failure (transient 502s, network blips)
// before giving up - so one bad page doesn't abandon a poll that had
// already successfully fetched 20+ other pages.
async function callGraphQLWithRetry(query, variables, maxAttempts = 3) {
  let lastError;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await callGraphQL(query, variables);
    } catch (err) {
      lastError = err;
      console.warn(`[poll] Page fetch attempt ${attempt}/${maxAttempts} failed: ${err.message}`);
      if (attempt < maxAttempts) {
        const backoffMs = attempt * 3000; // 3s, 6s, ...
        await sleep(backoffMs);
      }
    }
  }
  throw lastError;
}

async function fetchFullSchedule() {
  const from = new Date().toISOString();
  const to = SEASON_END_DATE
    ? new Date(SEASON_END_DATE + 'T23:59:59.999Z').toISOString()
    : new Date(Date.now() + POLL_WINDOW_DAYS * 24 * 60 * 60 * 1000).toISOString();

  let allEvents = [];
  let page = 1;
  let totalPages = 1;
  const PER_PAGE = parseInt(process.env.EVENTS_PER_PAGE || '40', 10); // 100/page hit "complexity 201, max 101" - roughly 2 complexity/game, so 40 leaves real margin under the ~50 edge
  const PAGE_DELAY_MS = parseInt(process.env.PAGE_DELAY_MS || '400', 10); // small gap between requests, to avoid hammering SportsEngine's API in rapid succession

  do {
    const data = await callGraphQLWithRetry(EVENTS_QUERY, { orgId: SE_ORG_ID, from, to, page, perPage: PER_PAGE });
    const pageResults = (data.events && data.events.results) || [];
    allEvents = allEvents.concat(pageResults);
    totalPages = (data.events && data.events.pageInformation && data.events.pageInformation.pages) || 1;
    console.log(`[poll] Fetched page ${page}/${totalPages} (${pageResults.length} events)`);
    page++;
    if (page <= totalPages) await sleep(PAGE_DELAY_MS);
  } while (page <= totalPages);

  return allEvents;
}

function extractGameInfo(event) {
  const teams = event.eventTeams || [];
  const home = teams.find((t) => t.homeTeam === true);
  const away = teams.find((t) => t.homeTeam === false);
  return {
    eventId: event.id,
    startTime: event.start || null,
    locationName: (event.subvenue && event.subvenue.name) || null,
    homeTeam: (home && home.name) || null,
    awayTeam: (away && away.name) || null,
    seUpdatedAt: event.updated || null,
    seCreatedAt: event.created || null,
  };
}

// ---------- CSV generation ----------

function csvEscape(val) {
  if (val == null) return '';
  const str = String(val);
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function formatDateTimeParts(isoString) {
  if (!isoString) return { date: '', time: '' };
  const d = new Date(isoString);
  const date = d.toLocaleDateString('en-US', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' });
  const time = d.toLocaleTimeString('en-US', { timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit' });
  return { date, time };
}

function generateCsv(changes) {
  const header = ['UUID', 'Date', 'Time', 'Location', 'Home Team', 'Away Team'];
  const lines = [header.join(',')];
  for (const c of changes) {
    const { date, time } = formatDateTimeParts(c.start_time);
    lines.push([
      csvEscape(c.event_id),
      csvEscape(date),
      csvEscape(time),
      csvEscape(c.location_name),
      csvEscape(c.home_team),
      csvEscape(c.away_team),
    ].join(','));
  }
  return lines.join('\n');
}

// A game whose `updated` sits only moments after its `created` is a brand
// new addition, not an edit - its "recent update" is just the creation
// itself. Only report it as a change if updated is meaningfully later than
// created (default 5 minutes - configurable, since this is a judgment call
// about what "meaningfully later" means for SportsEngine's own timestamps).
const NEW_GAME_THRESHOLD_MS = parseInt(process.env.NEW_GAME_THRESHOLD_MINUTES || '5', 10) * 60_000;

function isGenuineEdit(info) {
  if (!info.seUpdatedAt || !info.seCreatedAt) return true; // can't tell - err toward reporting it
  const updated = new Date(info.seUpdatedAt).getTime();
  const created = new Date(info.seCreatedAt).getTime();
  return (updated - created) > NEW_GAME_THRESHOLD_MS;
}

// ---------- Poll state (single row: last successful poll time) ----------

async function getLastPollTime() {
  const result = await pool.query('SELECT last_poll_time FROM poll_state WHERE id = 1');
  if (result.rowCount === 0 || !result.rows[0].last_poll_time) {
    // No prior successful poll recorded - fall back to a generous window
    // rather than ever looking back forever or not at all.
    return new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  }
  return new Date(result.rows[0].last_poll_time);
}

async function setLastPollTime(time, changeCount) {
  await pool.query(
    `INSERT INTO poll_state (id, last_poll_time, last_run_at, last_run_status, last_run_error, last_run_change_count)
     VALUES (1, $1, now(), 'success', NULL, $2)
     ON CONFLICT (id) DO UPDATE SET last_poll_time = EXCLUDED.last_poll_time, last_run_at = now(),
       last_run_status = 'success', last_run_error = NULL, last_run_change_count = EXCLUDED.last_run_change_count`,
    [time, changeCount]
  );
}

async function recordPollFailure(errorMessage) {
  await pool.query(
    `INSERT INTO poll_state (id, last_run_at, last_run_status, last_run_error)
     VALUES (1, now(), 'failed', $1)
     ON CONFLICT (id) DO UPDATE SET last_run_at = now(), last_run_status = 'failed', last_run_error = EXCLUDED.last_run_error`,
    [errorMessage]
  );
}

// ---------- Poll cycle ----------

async function runPoll() {
  console.log('[poll] Starting schedule poll...');
  const pollStartedAt = new Date(); // recorded BEFORE fetching, so nothing changed during this run is ever missed by the next one
  const lastPollTime = await getLastPollTime();
  console.log('[poll] Looking for changes updated since', lastPollTime.toISOString());

  let events;
  try {
    events = await fetchFullSchedule();
  } catch (err) {
    console.error('[poll] Failed to fetch schedule - NOT advancing last_poll_time, will retry the same window next run:', err.message);
    await recordPollFailure(err.message);
    return;
  }
  console.log(`[poll] Fetched ${events.length} total games. Filtering for genuine recent edits...`);

  let changeCount = 0;

  const DEBUG_EVENT_ID = process.env.DEBUG_EVENT_ID || null;

  for (const event of events) {
    const info = extractGameInfo(event);
    if (!info.eventId || !info.seUpdatedAt) continue;

    const updatedAt = new Date(info.seUpdatedAt);
    const notRecentEnough = updatedAt <= lastPollTime;
    const notGenuineEdit = !notRecentEnough && !isGenuineEdit(info);

    if (DEBUG_EVENT_ID && info.eventId === DEBUG_EVENT_ID) {
      console.log('[DEBUG]', JSON.stringify({
        eventId: info.eventId,
        raw_updated: info.seUpdatedAt,
        raw_created: info.seCreatedAt,
        parsed_updatedAt: updatedAt.toISOString(),
        lastPollTime: lastPollTime.toISOString(),
        notRecentEnough,
        notGenuineEdit,
        gapMinutes: info.seCreatedAt ? (updatedAt.getTime() - new Date(info.seCreatedAt).getTime()) / 60000 : null,
      }, null, 2));
    }

    if (notRecentEnough) continue; // not updated since our last check
    if (notGenuineEdit) continue; // looks like a new game, not an edit to an existing one

    const result = await pool.query(
      `INSERT INTO schedule_changes (event_id, start_time, location_name, home_team, away_team, se_updated_at, se_created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (event_id, start_time, location_name, home_team, away_team) DO NOTHING
       RETURNING id`,
      [info.eventId, info.startTime, info.locationName, info.homeTeam, info.awayTeam, info.seUpdatedAt, info.seCreatedAt]
    );
    if (result.rowCount > 0) changeCount++; // only counts genuinely new rows, not skipped duplicates
  }

  await setLastPollTime(pollStartedAt, changeCount);
  console.log(`[poll] Done. ${changeCount} new change(s) recorded (duplicates, if any, were skipped).`);
}

// ---------- Server: query the change log + manual trigger ----------


const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');

  // GET /api/status — last poll run's outcome, for the debug UI
  if (req.method === 'GET' && url.pathname === '/api/status') {
    try {
      const result = await pool.query('SELECT * FROM poll_state WHERE id = 1');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ status: result.rows[0] || null }));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  // GET /changes — list recorded changes as JSON, optionally filtered with ?since=<ISO date>
  if (req.method === 'GET' && url.pathname === '/changes') {
    try {
      const since = url.searchParams.get('since');
      const result = since
        ? await pool.query('SELECT * FROM schedule_changes WHERE detected_at >= $1 ORDER BY detected_at DESC', [since])
        : await pool.query('SELECT * FROM schedule_changes ORDER BY detected_at DESC LIMIT 500');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ changes: result.rows }));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ error: err.message }));
    }
  }

  // GET /changes.csv — same data, as a downloadable CSV, optionally filtered with ?since=<ISO date>
  if (req.method === 'GET' && url.pathname === '/changes.csv') {
    try {
      const since = url.searchParams.get('since');
      const result = since
        ? await pool.query('SELECT * FROM schedule_changes WHERE detected_at >= $1 ORDER BY detected_at DESC', [since])
        : await pool.query('SELECT * FROM schedule_changes ORDER BY detected_at DESC LIMIT 500');
      const csv = generateCsv(result.rows);
      res.writeHead(200, {
        'Content-Type': 'text/csv',
        'Content-Disposition': 'attachment; filename="schedule-changes.csv"',
      });
      return res.end(csv);
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      return res.end('Error: ' + err.message);
    }
  }

  // POST /trigger-poll — manual trigger, for testing without waiting for the schedule
  if (req.method === 'POST' && url.pathname === '/trigger-poll') {
    runPoll()
      .then(() => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true }));
      })
      .catch((err) => {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      });
    return;
  }

  // GET / — debug UI: status, manual trigger, recent changes
  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
    fs.readFile(HTML_FILE, 'utf8', (err, data) => {
      if (err) {
        res.writeHead(404);
        return res.end('index.html not found — make sure it is in the same folder as server.js');
      }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(data);
    });
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

server.listen(PORT, async () => {
  console.log(`Schedule monitor running on port ${PORT}`);
  console.log(`Poll interval: every ${POLL_INTERVAL_HOURS} hours, window: ${SEASON_END_DATE ? 'through ' + SEASON_END_DATE : 'next ' + POLL_WINDOW_DAYS + ' days'}`);
  if (!process.env.DATABASE_URL) {
    console.warn('WARNING: no DATABASE_URL set.');
  } else {
    try {
      await pool.query('SELECT 1');
      console.log('[postgres] Connected successfully.');
    } catch (err) {
      console.error('[postgres] Connection test FAILED:', err.message);
    }
  }

  runPoll(); // run once on startup
  setInterval(runPoll, POLL_INTERVAL_HOURS * 60 * 60 * 1000);
});
