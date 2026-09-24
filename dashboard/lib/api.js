const DEFAULT_BROKERS = [
  "http://localhost:8081",
  "http://localhost:8082",
  "http://localhost:8083",
];

export function brokerUrls() {
  const configured = process.env.NEXT_PUBLIC_BROKERS;
  const urls = configured ? configured.split(",") : DEFAULT_BROKERS;
  return urls.map((u) => u.trim()).filter(Boolean);
}

async function getJson(url, timeoutMs = 1500) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: controller.signal, cache: "no-store" });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

/**
 * One polling sweep across every configured broker: per-broker metrics and
 * cluster view, plus a lag report from the first broker that can serve one.
 */
export async function pollCluster() {
  const urls = brokerUrls();
  const brokers = await Promise.all(
    urls.map(async (url) => {
      const [metrics, cluster] = await Promise.all([
        getJson(`${url}/api/metrics`),
        getJson(`${url}/api/cluster`),
      ]);
      return { url, metrics, cluster };
    })
  );

  let lag = null;
  for (const broker of brokers) {
    if (!broker.metrics) continue;
    lag = await getJson(`${broker.url}/api/lag`, 2500);
    if (lag) break;
  }

  return { brokers, lag: lag ?? [] };
}

/**
 * Merges the per-broker cluster views into one topology: broker liveness from
 * the controller's perspective when available, and per-partition end offsets
 * taken as the max any replica reports (brokers report -1 for partitions they
 * do not host).
 */
export function mergeTopology(brokers) {
  const reachable = brokers.filter((b) => b.cluster);
  if (reachable.length === 0) return { nodes: [], topics: [], controllerId: -1, term: 0 };

  const authority =
    reachable.find((b) => b.cluster.role === "LEADER")?.cluster ?? reachable[0].cluster;

  const topicsByName = new Map();
  for (const broker of reachable) {
    for (const topic of broker.cluster.topics ?? []) {
      let merged = topicsByName.get(topic.name);
      if (!merged) {
        merged = {
          name: topic.name,
          partitionCount: topic.partitionCount,
          partitions: topic.partitions.map((p) => ({ ...p })),
        };
        topicsByName.set(topic.name, merged);
        continue;
      }
      topic.partitions.forEach((p, i) => {
        if (merged.partitions[i] && p.endOffset > merged.partitions[i].endOffset) {
          merged.partitions[i].endOffset = p.endOffset;
        }
      });
    }
  }

  return {
    nodes: authority.brokers ?? [],
    topics: [...topicsByName.values()],
    controllerId: authority.leaderId,
    term: authority.term,
  };
}

export const BROKER_COLORS = ["#ffb454", "#4fd6be", "#b28dff", "#f2617a", "#7fd1ff"];

export function brokerColor(id) {
  if (id == null || id < 0) return "#5c6773";
  return BROKER_COLORS[(id - 1 + BROKER_COLORS.length * 100) % BROKER_COLORS.length];
}
