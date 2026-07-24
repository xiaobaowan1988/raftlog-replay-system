// Thin client for the Golang backend.
//
// Base URL resolution:
//   - VITE_API_BASE, if set (e.g. "https://gitops-api.internal"), else
//   - "" (relative) → in dev this hits the Vite proxy (/api → :8080, see
//     vite.config.js); in prod it hits the same origin that serves the app.
const BASE = import.meta.env.VITE_API_BASE ?? '';

async function getJSON(path) {
  const res = await fetch(`${BASE}${path}`, { headers: { Accept: 'application/json' } });
  if (!res.ok) {
    throw new Error(`${path} → HTTP ${res.status}`);
  }
  return res.json();
}

// GET /api/tree → { services: [...], templates: [...] }
export const fetchTree = () => getJSON('/api/tree');

// GET /api/services/{id} → { ...meta, content: { source, merged, manifest } }
export const fetchService = (id) => getJSON(`/api/services/${encodeURIComponent(id)}`);

// POST /api/services/{id}/deploy — Server-Side Apply the rendered FlinkDeployment
// CR to the cluster. Pass { dryRun: true } to validate/admit without persisting.
// Throws with the backend's error message (e.g. deploy not configured) on failure.
export async function deployService(id, { dryRun = false } = {}) {
  const qs = dryRun ? '?dryRun=true' : '';
  const res = await fetch(`${BASE}/api/services/${encodeURIComponent(id)}/deploy${qs}`, {
    method: 'POST',
    headers: { Accept: 'application/json' },
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(body.error || `deploy → HTTP ${res.status}`);
  }
  return body; // { status, dryRun, applied: [{ kind, name, namespace }] }
}
