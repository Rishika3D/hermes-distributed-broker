// Server-side cluster polling. Runs inside the Next.js server (never in the browser), so the
// broker API key stays on the server and brokers need not be reachable from end users.
//
//   HERMES_BROKERS  comma-separated broker REST base URLs (default: three localhost nodes)
//   HERMES_API_KEY  sent as X-API-Key when the brokers have auth enabled

const DEFAULT_BROKERS = ["http://localhost:8081", "http://localhost:8082", "http://localhost:8083"];

export function brokerUrls(env = process.env) {
  const configured = env.HERMES_BROKERS;
  const urls = configured ? configured.split(",") : DEFAULT_BROKERS;
  return urls.map((u) => u.trim().replace(/\/+$/, "")).filter(Boolean);
}

async function getJson(url, { apiKey, timeoutMs = 1500 } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: controller.signal,
      cache: "no-store",
      headers: apiKey ? { "X-API-Key": apiKey } : {},
    });
    return res.ok ? await res.json() : null;
  } catch {
    return null; // unreachable / timed out / bad JSON: the dashboard shows the node as down
  } finally {
    clearTimeout(timer);
  }
}

/**
 * One polling sweep across every configured broker: per-broker metrics and cluster view,
 * plus a lag report from the first reachable broker that can serve one. Never throws; an
 * unreachable broker simply has null metrics/cluster.
 */
export async function pollCluster(env = process.env) {
  const urls = brokerUrls(env);
  const apiKey = env.HERMES_API_KEY || undefined;

  const brokers = await Promise.all(
    urls.map(async (url) => {
      const [metrics, cluster] = await Promise.all([
        getJson(`${url}/api/metrics`, { apiKey }),
        getJson(`${url}/api/cluster`, { apiKey }),
      ]);
      return { url, metrics, cluster };
    })
  );

  let lag = null;
  for (const broker of brokers) {
    if (!broker.metrics) continue;
    lag = await getJson(`${broker.url}/api/lag`, { apiKey, timeoutMs: 2500 });
    if (lag) break;
  }

  return { brokers, lag: lag ?? [] };
}
