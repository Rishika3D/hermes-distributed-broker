import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";

import { brokerUrls, pollCluster } from "../lib/poll.js";
import { mergeTopology } from "../lib/api.js";

// A stub broker REST API. When `requiredKey` is set it rejects requests lacking X-API-Key,
// exactly like the real ApiKeyFilter.
function stubBroker({ requiredKey, metrics, cluster, lag }) {
  const seenKeys = [];
  const server = http.createServer((req, res) => {
    seenKeys.push(req.headers["x-api-key"]);
    if (requiredKey && req.headers["x-api-key"] !== requiredKey) {
      res.writeHead(401, { "Content-Type": "application/json" });
      return res.end('{"error":"missing or invalid X-API-Key"}');
    }
    const body = { "/api/metrics": metrics, "/api/cluster": cluster, "/api/lag": lag }[req.url];
    res.writeHead(body ? 200 : 404, { "Content-Type": "application/json" });
    res.end(JSON.stringify(body ?? {}));
  });
  return new Promise((resolve) =>
    server.listen(0, "127.0.0.1", () =>
      resolve({ server, url: `http://127.0.0.1:${server.address().port}`, seenKeys })
    )
  );
}

const METRICS = { brokerId: 1, role: "LEADER", messagesInPerSec: 5 };
const CLUSTER = { leaderId: 1, term: 2, brokers: [{ id: 1, alive: true }], topics: [] };
const LAG = [{ group: "g", topic: "t", partition: 0, lag: 3 }];

let open = [];
after(() => Promise.all(open.map((s) => new Promise((r) => s.close(r)))));
async function broker(opts) {
  const b = await stubBroker(opts);
  open.push(b.server);
  return b;
}

test("brokerUrls: defaults, trimming, trailing slashes, blanks", () => {
  assert.equal(brokerUrls({}).length, 3);
  assert.deepEqual(brokerUrls({ HERMES_BROKERS: " http://a:1/ , http://b:2// ,," }), [
    "http://a:1",
    "http://b:2",
  ]);
});

test("pollCluster sends the API key and reads an auth-protected broker as healthy", async () => {
  const b = await broker({ requiredKey: "k3y", metrics: METRICS, cluster: CLUSTER, lag: LAG });
  const snap = await pollCluster({ HERMES_BROKERS: b.url, HERMES_API_KEY: "k3y" });

  assert.equal(snap.brokers.length, 1);
  assert.deepEqual(snap.brokers[0].metrics, METRICS);
  assert.deepEqual(snap.lag, LAG);
  assert.ok(b.seenKeys.every((k) => k === "k3y"), "every request must carry the key");
});

test("regression: without the key an auth-protected broker looks down (old dashboard behaviour)", async () => {
  const b = await broker({ requiredKey: "k3y", metrics: METRICS, cluster: CLUSTER, lag: LAG });
  const snap = await pollCluster({ HERMES_BROKERS: b.url });
  assert.equal(snap.brokers[0].metrics, null);
});

test("an unreachable broker yields nulls instead of throwing", async () => {
  // port 1 is reserved/closed: connection refused
  const snap = await pollCluster({ HERMES_BROKERS: "http://127.0.0.1:1" });
  assert.equal(snap.brokers[0].metrics, null);
  assert.equal(snap.brokers[0].cluster, null);
  assert.deepEqual(snap.lag, []);
});

test("one dead broker does not hide the live ones; lag comes from a live broker", async () => {
  const live = await broker({ metrics: METRICS, cluster: CLUSTER, lag: LAG });
  const snap = await pollCluster({ HERMES_BROKERS: `http://127.0.0.1:1,${live.url}` });

  assert.equal(snap.brokers[0].metrics, null);
  assert.deepEqual(snap.brokers[1].metrics, METRICS);
  assert.deepEqual(snap.lag, LAG);
});

test("mergeTopology: empty input", () => {
  assert.deepEqual(mergeTopology([]), { nodes: [], topics: [], controllerId: -1, term: 0 });
});

test("mergeTopology: prefers the controller's view and takes the max end offset per partition", () => {
  const topic = (end) => ({
    name: "t",
    partitionCount: 1,
    partitions: [{ partition: 0, leader: 1, replicas: [1, 2], endOffset: end }],
  });
  const follower = { cluster: { role: "FOLLOWER", leaderId: 1, term: 3, brokers: [{ id: 9 }], topics: [topic(-1)] } };
  const leader = { cluster: { role: "LEADER", leaderId: 1, term: 3, brokers: [{ id: 1 }], topics: [topic(42)] } };

  const merged = mergeTopology([follower, leader]);
  assert.equal(merged.controllerId, 1);
  assert.deepEqual(merged.nodes, [{ id: 1 }], "node list comes from the controller");
  assert.equal(merged.topics[0].partitions[0].endOffset, 42, "-1 (not hosted) must not win");
});
