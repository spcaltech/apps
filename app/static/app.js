const qs = (s) => document.querySelector(s);
const qsa = (s) => Array.from(document.querySelectorAll(s));

const state = {
  files: [],
  jobId: null,
};

const formatBytes = (n) => {
  if (n === 0 || n == null) return "-";
  const units = ["B", "KB", "MB", "GB", "TB"]; 
  const e = Math.floor(Math.log(n) / Math.log(1024));
  return `${(n / Math.pow(1024, e)).toFixed(1)} ${units[e]}`;
};

const recommendedPatterns = [
  /config\.json$/,
  /tokenizer\.(json|model)$/,
  /(merges\.txt|vocab\.json)$/,
  /(pytorch_model|model)\.(bin|safetensors)(\.index)?$/,
];

function renderFiles() {
  const list = qs('#filesList');
  list.innerHTML = '';
  state.files.forEach((f, idx) => {
    const row = document.createElement('label');
    row.className = 'file-row';
    row.innerHTML = `
      <input type=\"checkbox\" class=\"file-check\" data-idx=\"${idx}\" />
      <span class=\"path\">${f.path}</span>
      <span class=\"size\">${formatBytes(f.size)}</span>
    `;
    list.appendChild(row);
  });
  qs('#filesSection').classList.toggle('hidden', state.files.length === 0);
  qs('#btnPrefetch').disabled = state.files.length === 0;
}

function selectRecommended() {
  const checks = qsa('.file-check');
  checks.forEach((c) => {
    const idx = Number(c.dataset.idx);
    const path = state.files[idx]?.path || '';
    c.checked = recommendedPatterns.some((re) => re.test(path));
  });
}

function selectAll(v) {
  qsa('.file-check').forEach((c) => (c.checked = v));
}

async function loadFiles() {
  const repoId = qs('#repoId').value.trim();
  const revision = qs('#revision').value.trim();
  if (!repoId) {
    alert('Enter a model repo id');
    return;
  }
  qs('#btnLoadFiles').disabled = true;
  try {
    const resp = await fetch(`/api/model-files?repo_id=${encodeURIComponent(repoId)}${revision ? `&revision=${encodeURIComponent(revision)}` : ''}`);
    if (!resp.ok) throw new Error(await resp.text());
    const data = await resp.json();
    state.files = data.files || [];
    renderFiles();
  } catch (e) {
    alert(`Failed to load files: ${e}`);
  } finally {
    qs('#btnLoadFiles').disabled = false;
  }
}

async function startPrefetch() {
  const repoId = qs('#repoId').value.trim();
  const revision = qs('#revision').value.trim() || null;
  const projects = qs('#projects').value.split(',').map((s) => s.trim()).filter(Boolean);
  const selected = qsa('.file-check').map((c) => ({c, idx: Number(c.dataset.idx)})).filter(({c}) => c.checked).map(({idx}) => state.files[idx].path);
  if (!repoId || projects.length === 0 || selected.length === 0) {
    alert('Provide repo, at least one project, and select files.');
    return;
  }
  qs('#btnPrefetch').disabled = true;
  try {
    const resp = await fetch('/api/prefetch', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ repo_id: repoId, revision, project_names: projects, files: selected })
    });
    if (!resp.ok) throw new Error(await resp.text());
    const data = await resp.json();
    state.jobId = data.job_id;
    qs('#statusCard').classList.remove('hidden');
    pollStatus();
  } catch (e) {
    alert(`Failed to start prefetch: ${e}`);
    qs('#btnPrefetch').disabled = false;
  }
}

async function pollStatus() {
  if (!state.jobId) return;
  try {
    const resp = await fetch(`/api/status/${state.jobId}`);
    if (!resp.ok) throw new Error(await resp.text());
    const s = await resp.json();
    qs('#statusText').textContent = `${s.status}${s.message ? ` - ${s.message}` : ''} (${s.downloaded_files}/${s.total_files})`;
    const pct = Math.round((s.progress || 0) * 100);
    qs('#progressBar').style.width = pct + '%';
    if (s.status === 'completed' || s.status === 'failed') {
      qs('#btnPrefetch').disabled = false;
      return;
    }
    setTimeout(pollStatus, 1000);
  } catch (e) {
    qs('#statusText').textContent = `Error: ${e}`;
    qs('#btnPrefetch').disabled = false;
  }
}

qs('#btnLoadFiles').addEventListener('click', loadFiles);
qs('#btnPrefetch').addEventListener('click', startPrefetch);
qs('#btnSelectRecommended').addEventListener('click', (e) => { e.preventDefault(); selectRecommended(); });
qs('#btnSelectAll').addEventListener('click', (e) => { e.preventDefault(); selectAll(true); });
qs('#btnClear').addEventListener('click', (e) => { e.preventDefault(); selectAll(false); });