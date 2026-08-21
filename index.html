<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Schedule Monitor — Debug</title>
  <style>
    body { font-family: -apple-system, sans-serif; background: #F4F6FA; margin: 0; color: #1a1a1a; }
    header { background: #11213F; color: #E7C247; padding: 14px 20px; font-weight: 700; font-size: 17px; }
    main { max-width: 900px; margin: 0 auto; padding: 24px; }
    .card { background: #fff; border: 1px solid #E1E5EC; border-radius: 10px; padding: 20px; margin-bottom: 18px; }
    .muted { color: #5A6472; font-size: 13px; }
    button { padding: 10px 18px; border-radius: 6px; border: none; background: #11213F; color: #fff; font-weight: 600; cursor: pointer; font-size: 14px; }
    button:disabled { opacity: .5; cursor: not-allowed; }
    .badge { display: inline-block; padding: 3px 10px; border-radius: 10px; font-size: 11px; font-weight: 700; text-transform: uppercase; }
    .badge-success { background: #E9F6EE; color: #1F7A4D; }
    .badge-failed { background: #FBEAE8; color: #C0392B; }
    table { width: 100%; border-collapse: collapse; margin-top: 10px; }
    th { text-align: left; font-size: 11px; text-transform: uppercase; color: #5A6472; padding: 6px 8px; border-bottom: 2px solid #E1E5EC; }
    td { padding: 8px; border-bottom: 1px solid #E1E5EC; font-size: 13.5px; }
    .err-box { background: #FBEAE8; color: #C0392B; padding: 10px 14px; border-radius: 6px; margin-top: 10px; font-size: 13px; white-space: pre-wrap; }
  </style>
</head>
<body>
<header>Schedule Monitor — Debug</header>
<main>
  <div class="card">
    <div style="display:flex; align-items:center; justify-content:space-between;">
      <h3 style="margin:0;">Status</h3>
      <button id="triggerBtn" onclick="triggerPoll()">Trigger Poll Now</button>
    </div>
    <div id="statusBox" style="margin-top:14px;">Loading...</div>
  </div>

  <div class="card">
    <div style="display:flex; align-items:center; justify-content:space-between;">
      <h3 style="margin:0;">Changes Detected — Last 24 Hours</h3>
      <a href="#" id="csvLink" class="muted">Download CSV</a>
    </div>
    <div id="changesBox">Loading...</div>
  </div>
</main>
<script>
  let lastKnownRunAt = null; // used to detect when a NEW poll (scheduled or manual) has actually completed

  async function loadStatus() {
    const res = await fetch('/api/status');
    const data = await res.json();
    const s = data.status;
    const box = document.getElementById('statusBox');
    if (!s || !s.last_run_at) {
      box.innerHTML = '<p class="muted">No poll has run yet.</p>';
      return false;
    }
    const badge = s.last_run_status === 'success'
      ? '<span class="badge badge-success">success</span>'
      : '<span class="badge badge-failed">failed</span>';
    box.innerHTML = `
      <p>${badge} — last run at ${new Date(s.last_run_at).toLocaleString()}</p>
      ${s.last_run_status === 'success' ? '<p class="muted">' + (s.last_run_change_count ?? 0) + ' change(s) recorded on last run.</p>' : ''}
      ${s.last_run_error ? '<div class="err-box">' + escapeHtml(s.last_run_error) + '</div>' : ''}
    `;

    // A genuinely NEW poll completed (not just the first check on page load)
    const isNewCompletion = lastKnownRunAt !== null && s.last_run_at !== lastKnownRunAt;
    lastKnownRunAt = s.last_run_at;
    return isNewCompletion;
  }

  async function loadChanges() {
    const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    document.getElementById('csvLink').href = '/changes.csv?since=' + encodeURIComponent(since);
    const res = await fetch('/changes?since=' + encodeURIComponent(since));
    const data = await res.json();
    const box = document.getElementById('changesBox');
    if (!data.changes || data.changes.length === 0) {
      box.innerHTML = '<p class="muted">No changes detected in the last 24 hours.</p>';
      return;
    }
    box.innerHTML = `
      <table>
        <thead><tr><th>Detected</th><th>Updated (SE)</th><th>UUID</th><th>Start</th><th>Location</th><th>Home</th><th>Away</th></tr></thead>
        <tbody>
          ${data.changes.map(c => `
            <tr>
              <td>${new Date(c.detected_at).toLocaleString()}</td>
              <td>${c.se_updated_at ? new Date(c.se_updated_at).toLocaleString() : ''}</td>
              <td style="font-size:11px;">${escapeHtml(c.event_id)}</td>
              <td>${c.start_time ? new Date(c.start_time).toLocaleString() : ''}</td>
              <td>${escapeHtml(c.location_name || '')}</td>
              <td>${escapeHtml(c.home_team || '')}</td>
              <td>${escapeHtml(c.away_team || '')}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    `;
  }

  async function triggerPoll() {
    const btn = document.getElementById('triggerBtn');
    btn.disabled = true;
    btn.textContent = 'Polling...';
    try {
      const res = await fetch('/trigger-poll', { method: 'POST' });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || 'Unknown error');
      await loadStatus();
      await loadChanges();
    } catch (e) {
      alert('Trigger failed: ' + e.message);
    } finally {
      btn.disabled = false;
      btn.textContent = 'Trigger Poll Now';
    }
  }

  function escapeHtml(str) {
    return String(str == null ? '' : str).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  }

  // Initial load: establish the baseline last_run_at (no refresh triggered
  // by this first check) and show whatever's currently in the table.
  loadStatus();
  loadChanges();

  // Check status periodically - but only refresh the CHANGES TABLE when a
  // genuinely new poll (scheduled or manual) has actually completed since
  // the last check, not on every tick regardless of whether anything ran.
  setInterval(async () => {
    const pollJustCompleted = await loadStatus();
    if (pollJustCompleted) {
      await loadChanges();
    }
  }, 60_000);
</script>
</body>
</html>
