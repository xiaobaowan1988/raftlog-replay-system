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
