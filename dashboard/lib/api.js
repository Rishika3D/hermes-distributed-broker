/**
 * Fetches the latest cluster snapshot from this app's own server-side poller (/api/poll).
 * Never throws: if even the dashboard server is unreachable, returns an empty snapshot so
 * the UI shows its "cluster unreachable" state instead of crashing.
 */
export async function fetchSnapshot(timeoutMs = 6000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch("/api/poll", { signal: controller.signal, cache: "no-store" });
    if (!res.ok) throw new Error(`poll responded ${res.status}`);
    return await res.json();
  } catch {
    return { brokers: [], lag: [] };
  } finally {
    clearTimeout(timer);
  }
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
