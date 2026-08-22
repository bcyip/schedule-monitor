// USCCS Schedule Change Monitor
//
// Polls SportsEngine's full game schedule on a schedule (default 3x/day)
// and records any game whose `updated` timestamp falls within the last 24
// hours - recomputed fresh on every run (not "since last poll").
//
// KNOWN, DELIBERATE TRADEOFF (see conversation): SportsEngine's `updated`
// timestamp on an event also moves for reasons unrelated to
// date/time/location - most likely team/roster changes cascading onto the
// event object. This means some reported "changes" will be false
// positives where the actual schedule didn't move. A field-level diffing
// approach (comparing actual start_time/location_name against a stored
// snapshot) would eliminate this, but was deliberately not used here in
// favor of simplicity.
//
// Durable log: every detected change accumulates in schedule_changes. A
// UNIQUE constraint + ON CONFLICT DO NOTHING guarantees the exact same
// (event_id, start_time, location_name, home_team, away_team) combination
// is never recorded twice, even though the fixed 24h window means the same
// still-recent game gets re-evaluated on every subsequent poll within that
// window.
//
// REQUIRED ENVIRONMENT VARIABLES:
//   SE_CLIENT_ID, SE_CLIENT_SECRET, SE_REFRESH_TOKEN, SE_ORG_ID - same as the other apps
//   DATABASE_URL              - the schedule monitor's OWN Supabase Postgres instance
//   POLL_INTERVAL_HOURS       - (optional) defaults to 8 (3x/day)
//   SEASON_END_DATE           - (optional but preferred) e.g. "2026-11-15" - polls exactly
//                                through the real season end, never drifting past it or
//                                falling short. Takes priority over POLL_WINDOW_DAYS if set.
//   POLL_WINDOW_DAYS          - (optional) fallback only, used if SEASON_END_DATE isn't set - defaults to 100
//   CLEAR_REPOLL_PASSWORD     - required to use the destructive "Clear & Re-poll"
//                                action - fails closed (disabled) if unset
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

// Division ID -> {name, gender} lookup, reused directly from the
// original stat-tracking app's confirmed DIVISIONS_MEN/DIVISIONS_WOMEN
// lists (45 Men's + 16 Women's = 61 total, verified earlier in this
// project) - more reliable than parsing a season name string, and
// needs no additional GraphQL query fields.
const DIVISION_LOOKUP = {
  '6a4439745407815052443199': { name: 'AL/MS', gender: 'Men' },
  '6a4439745407813fac44341f': { name: 'Baltimore', gender: 'Men' },
  '6a0c980026aee381f43b3ef0': { name: 'Big Sky', gender: 'Men' },
  '6a4e96e416115e0153c5de9d': { name: 'Crossroads', gender: 'Men' },
  '6a44397409f291d00804fde8': { name: 'Florida North', gender: 'Men' },
  '6a4439744e42a26275c7ff31': { name: 'Florida South', gender: 'Men' },
  '6a443974b5457c8f19bccbfc': { name: 'Georgia', gender: 'Men' },
  '6a4e96e4c7769d01229fb5c6': { name: 'Great Lakes', gender: 'Men' },
  '6a4e96e47d61ac00f01ad8b8': { name: 'Great Lakes II', gender: 'Men' },
  '6a4e96e4801326012116f744': { name: 'Great Plains', gender: 'Men' },
  '6a4e96e46b8d1e00ef1ee690': { name: 'Heartland', gender: 'Men' },
  '6a4e96e416115e0122c5e2c1': { name: 'Heartland II', gender: 'Men' },
  '6a44397409f291e60904fdda': { name: 'Hudson Valley', gender: 'Men' },
  '6a443974b5457c54e4bcd12d': { name: 'KY/TN', gender: 'Men' },
  '6a0cb110cdb76433cc151485': { name: 'Midwest North', gender: 'Men' },
  '6a0e070026aee3f6e43b446f': { name: 'Midwest North II', gender: 'Men' },
  '6a0e0700cdb76416601513ae': { name: 'Midwest South', gender: 'Men' },
  '6a4e96e46b8d1e01201ee2ce': { name: 'Midwest South II', gender: 'Men' },
  '6a44397409f29192050500f1': { name: 'NC East', gender: 'Men' },
  '6a4439744e42a29553c7fe6c': { name: 'NC West', gender: 'Men' },
  '6a4439743c1d137b18c8db12': { name: 'New England Central', gender: 'Men' },
  '6a443974593ddf505e16a197': { name: 'New England North', gender: 'Men' },
  '6a44397454078160b344310a': { name: 'New England South', gender: 'Men' },
  '6a0c980026aee362b23b3f0e': { name: 'NorCal', gender: 'Men' },
  '6a0c980004d6b0c7a8af73bd': { name: 'NorCal II', gender: 'Men' },
  '6a0c980098c2fde79e7c20e8': { name: 'Northwest', gender: 'Men' },
  '6a0e08bbf841cfba91996998': { name: 'Northwoods', gender: 'Men' },
  '6a0e09e298c2fd5b067c22c4': { name: 'Northwoods II', gender: 'Men' },
  '6a4439744e42a24ee3c80243': { name: 'NYC', gender: 'Men' },
  '6a443974b5457c6597bccdad': { name: 'Philly', gender: 'Men' },
  '6a0e08bb61444a4db0f2ef89': { name: 'Prairie', gender: 'Men' },
  '6a4e96e48dabb800efcfbce9': { name: 'Red River', gender: 'Men' },
  '6a4e96e48dabb80151cfb8bd': { name: 'Red River II', gender: 'Men' },
  '6a4e96e4c7769d00f19fb81b': { name: 'Rocky Mountain', gender: 'Men' },
  '6a4e96e435cc6d00ef70bd6d': { name: 'Rocky Mountain II', gender: 'Men' },
  '6a4e96e4c7769d00bc9fbb3f': { name: 'Sabine River', gender: 'Men' },
  '6a4e96e48dabb80120cfb96d': { name: 'Sabine River II', gender: 'Men' },
  '6a0c980026aee3a5643b3edf': { name: 'SoCal', gender: 'Men' },
  '6a0c980098c2fd32fc7c1d07': { name: 'SoCal II', gender: 'Men' },
  '6a3ed52cf2a55d01e50c59e5': { name: 'SoCal III', gender: 'Men' },
  '6a4e96e480132600f016f99d': { name: 'Southwest', gender: 'Men' },
  '6a4e96e4c7769d01539fb593': { name: 'Southwest II', gender: 'Men' },
  '6a0c9800bc500ed1f8da9d08': { name: 'Utah', gender: 'Men' },
  '6a443974593ddf60ee169e3f': { name: 'Virginia', gender: 'Men' },
  '6a44397409f291bc4904fe1b': { name: 'Washington DC', gender: 'Men' },
  '6a4446b9c3ff52e2d366a921': { name: 'DMV', gender: 'Women' },
  '6a444639c3ff52b71466af8b': { name: 'FL', gender: 'Women' },
  '6a0e0a64a70302e718b84569': { name: 'Midwest', gender: 'Women' },
  '6a0e0a6498c2fd83787c1db1': { name: 'Midwest II', gender: 'Women' },
  '6a46cdfc06f455aa0cc3badb': { name: 'New England', gender: 'Women' },
  '6a0c982c61444a4e0cf2efa3': { name: 'NorCal', gender: 'Women' },
  '6a0c982c61444a7966f2eb9a': { name: 'NorCal II', gender: 'Women' },
  '6a0c982cf841cf9b0499667d': { name: 'Northwest', gender: 'Women' },
  '6a0c982c98c2fd32fc7c1d0d': { name: 'Oregon II', gender: 'Women' },
  '6a4e9b904b609600f0e70d42': { name: 'Ozark', gender: 'Women' },
  '6a44470e99ca5a7f419d52d3': { name: 'Philly', gender: 'Women' },
  '6a4e9b6a801326012116f792': { name: 'Rocky Mountain', gender: 'Women' },
  '6a4e9b6a4b60960121e70967': { name: 'Rocky Mountain II', gender: 'Women' },
  '6a0c982c98c2fd79027c1c4f': { name: 'SoCal', gender: 'Women' },
  '6a0c982c8a5826dcee7f909e': { name: 'SoCal II', gender: 'Women' },
  '6a4e9b6a80132600f016fa7f': { name: 'Southwest', gender: 'Women' },
};

const EVENTS_QUERY = `
  query Events($orgId: Int!, $from: UTCDateTime!, $to: UTCDateTime!, $page: Int!, $perPage: Int!) {
    events(organizationId: $orgId, from: $from, to: $to, calendarEventType: GAME, page: $page, perPage: $perPage) {
      results {
        id
        eventTeams { name team { id program { primaryName } divisionId } homeTeam }
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
  const PAGE_DELAY_MS = parseInt(process.env.PAGE_DELAY_MS || '1000', 10); // bumped from 400ms after hitting confirmed TOO_MANY_REQUESTS from SportsEngine

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
  const subvenue = event.subvenue || {};
  const locationName = [subvenue.venueName, subvenue.name].filter(Boolean).join(' - ') || null;

  // Division comes from the home team specifically - both teams in a
  // standard divisional matchup share the same division, but if this is
  // ever a cross-division/exhibition game, home's division is used as the
  // representative one for this game.
  const divisionId = (home && home.team && home.team.divisionId) || (away && away.team && away.team.divisionId) || null;
  const divisionInfo = divisionId ? DIVISION_LOOKUP[divisionId] : null;

  return {
    eventId: event.id,
    startTime: event.start || null,
    locationName,
    subvenueId: event.subvenueId || null,
    venueId: subvenue.venueId || null,
    homeTeam: (home && home.name) || null,
    awayTeam: (away && away.name) || null,
    homeTeamId: (home && home.team && home.team.id) || null,
    awayTeamId: (away && away.team && away.team.id) || null,
    divisionId,
    divisionName: (divisionInfo && divisionInfo.name) || null,
    gender: (divisionInfo && divisionInfo.gender) || null,
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

function formatUTCParts(isoString) {
  if (!isoString) return { date: '', time: '' };
  const d = new Date(isoString);
  const date = d.toLocaleDateString('en-US', { timeZone: 'UTC', year: 'numeric', month: '2-digit', day: '2-digit' });
  const time = d.toLocaleTimeString('en-US', { timeZone: 'UTC', hour: '2-digit', minute: '2-digit' }) + ' UTC';
  return { date, time };
}

function formatEasternParts(isoString) {
  if (!isoString) return { date: '', time: '' };
  const d = new Date(isoString);
  const date = d.toLocaleDateString('en-US', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' });
  const time = d.toLocaleTimeString('en-US', { timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit', timeZoneName: 'short' });
  return { date, time };
}

function generateCsv(changes) {
  const header = ['UUID', 'New Game', 'Date (UTC)', 'Time (UTC)', 'Date (Eastern)', 'Time (Eastern)', 'Created (UTC)', 'Gender', 'Division', 'Division UUID', 'Location', 'Subvenue UUID', 'Venue UUID', 'Home Team', 'Home Team UUID', 'Away Team', 'Away Team UUID'];
  const lines = [header.join(',')];
  for (const c of changes) {
    const utc = formatUTCParts(c.start_time);
    const eastern = formatEasternParts(c.start_time);
    const created = formatUTCParts(c.se_created_at);
    lines.push([
      csvEscape(c.event_id),
      csvEscape(c.is_new_game ? 'Yes' : 'No'),
      csvEscape(utc.date),
      csvEscape(utc.time),
      csvEscape(eastern.date),
      csvEscape(eastern.time),
      csvEscape(created.date + ' ' + created.time),
      csvEscape(c.gender),
      csvEscape(c.division_name),
      csvEscape(c.division_id),
      csvEscape(c.location_name),
      csvEscape(c.subvenue_id),
      csvEscape(c.venue_id),
      csvEscape(c.home_team),
      csvEscape(c.home_team_id),
      csvEscape(c.away_team),
      csvEscape(c.away_team_id),
    ].join(','));
  }
  return lines.join('\n');
}

// ---------- Poll run status tracking (for the status UI) ----------

async function recordPollSuccess(changeCount) {
  await pool.query(
    `INSERT INTO poll_state (id, last_run_at, last_run_status, last_run_error, last_run_change_count)
     VALUES (1, now(), 'success', NULL, $1)
     ON CONFLICT (id) DO UPDATE SET last_run_at = now(), last_run_status = 'success', last_run_error = NULL, last_run_change_count = EXCLUDED.last_run_change_count`,
    [changeCount]
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
//
// Simple, fixed rolling window: on every run, fetch the full schedule and
// record any game whose SportsEngine `updated` timestamp falls within the
// last 24 hours - recomputed fresh each time, not "since last poll." No
// snapshot, no field-level diffing.
//
// KNOWN TRADEOFF (deliberate, see conversation): `updated` also moves for
// reasons unrelated to date/time/location - e.g. roster changes cascading
// onto the event object - so this WILL report some false positives when
// that happens. Chosen deliberately over field-diffing for simplicity.

async function runPoll() {
  console.log('[poll] Starting schedule poll...');
  const since = new Date(Date.now() - 24 * 60 * 60 * 1000);

  let events;
  try {
    events = await fetchFullSchedule();
  } catch (err) {
    console.error('[poll] Failed to fetch schedule:', err.message);
    await recordPollFailure(err.message);
    return;
  }
  console.log(`[poll] Fetched ${events.length} total games. Filtering for updated OR created within the last 24h (since ${since.toISOString()})...`);

  let changeCount = 0;

  for (const event of events) {
    const info = extractGameInfo(event);
    if (!info.eventId) continue;

    const updatedAt = info.seUpdatedAt ? new Date(info.seUpdatedAt) : null;
    const createdAt = info.seCreatedAt ? new Date(info.seCreatedAt) : null;

    // Checked independently, not as a replacement for each other - a brand
    // new game's `updated` may not reliably reflect its creation (e.g. if
    // it's set via a bulk import/template that doesn't touch `updated` at
    // that moment), so `created` is a second, separate way to catch it.
    const updatedRecently = updatedAt && updatedAt > since;
    const createdRecently = createdAt && createdAt > since;
    if (!updatedRecently && !createdRecently) continue; // neither signal is recent - skip

    const result = await pool.query(
      `INSERT INTO schedule_changes (event_id, start_time, location_name, subvenue_id, venue_id, home_team, away_team, home_team_id, away_team_id, division_id, division_name, gender, se_updated_at, se_created_at, is_new_game)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
       ON CONFLICT (event_id, start_time, location_name, home_team, away_team) DO NOTHING
       RETURNING id`,
      [info.eventId, info.startTime, info.locationName, info.subvenueId, info.venueId, info.homeTeam, info.awayTeam, info.homeTeamId, info.awayTeamId, info.divisionId, info.divisionName, info.gender, info.seUpdatedAt, info.seCreatedAt, createdRecently]
    );
    if (result.rowCount > 0) changeCount++; // only counts genuinely new rows, not skipped duplicates
  }

  await recordPollSuccess(changeCount);
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

  // POST /clear-and-repoll — testing convenience: wipes the entire change
  // log, then immediately runs a fresh poll. Destructive and irreversible -
  // the frontend confirms with the user AND requires a password before
  // ever calling this, and the password is verified here too (not just
  // client-side, which could be trivially bypassed by calling this
  // endpoint directly). Fails closed: if CLEAR_REPOLL_PASSWORD isn't set
  // on the deployment, every request is rejected rather than silently
  // allowing unprotected access.
  if (req.method === 'POST' && url.pathname === '/clear-and-repoll') {
    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', async () => {
      let payload;
      try {
        payload = JSON.parse(body || '{}');
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ error: 'Invalid JSON body' }));
      }

      const requiredPassword = process.env.CLEAR_REPOLL_PASSWORD;
      if (!requiredPassword) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ error: 'CLEAR_REPOLL_PASSWORD is not configured on the server - this destructive action is disabled until it is set.' }));
      }
      if (payload.password !== requiredPassword) {
        res.writeHead(403, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ error: 'Incorrect password.' }));
      }

      try {
        await pool.query('TRUNCATE schedule_changes');
        console.log('[clear-and-repoll] Password verified. schedule_changes truncated, running fresh poll...');
        await runPoll();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true }));
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
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
