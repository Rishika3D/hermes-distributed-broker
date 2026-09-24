"use client";

import { useEffect, useRef, useState } from "react";
import { pollCluster, mergeTopology } from "@/lib/api";
import MetricCard from "@/components/MetricCard";
import BrokerHealth from "@/components/BrokerHealth";
import ThroughputChart from "@/components/ThroughputChart";
import PartitionMap from "@/components/PartitionMap";
import LagTable from "@/components/LagTable";

const POLL_MS = 2000;
const HISTORY_POINTS = 90;

function formatMicros(micros) {
  if (micros >= 1000) return `${(micros / 1000).toFixed(1)} ms`;
  return `${micros} µs`;
}

function formatCount(n) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

export default function Dashboard() {
  const [snapshot, setSnapshot] = useState(null);
  const [history, setHistory] = useState([]);
  const [lastSweep, setLastSweep] = useState(null);
  const timer = useRef(null);

  useEffect(() => {
    let cancelled = false;
    async function sweep() {
      const data = await pollCluster();
      if (cancelled) return;
      setSnapshot(data);
      setLastSweep(new Date());
      const up = data.brokers.filter((b) => b.metrics);
      setHistory((prev) => {
        const point = {
          t: Date.now(),
          inRate: up.reduce((s, b) => s + b.metrics.messagesInPerSec, 0),
          outRate: up.reduce((s, b) => s + b.metrics.messagesOutPerSec, 0),
        };
        return [...prev, point].slice(-HISTORY_POINTS);
      });
      timer.current = setTimeout(sweep, POLL_MS);
    }
    sweep();
    return () => {
      cancelled = true;
      clearTimeout(timer.current);
    };
  }, []);

  if (!snapshot) {
    return (
      <main className="shell">
        <Masthead controllerId={-1} term={0} aliveCount={0} total={0} lastSweep={null} />
        <div className="empty">contacting cluster…</div>
      </main>
    );
  }

  const up = snapshot.brokers.filter((b) => b.metrics);
  const downUrls = snapshot.brokers.filter((b) => !b.metrics).map((b) => b.url);
  const topology = mergeTopology(snapshot.brokers);

  const aliveById = {};
  for (const node of topology.nodes) {
    aliveById[node.id] = node.alive;
  }

  const inRate = up.reduce((s, b) => s + b.metrics.messagesInPerSec, 0);
  const outRate = up.reduce((s, b) => s + b.metrics.messagesOutPerSec, 0);
  const p99 = Math.max(0, ...up.map((b) => b.metrics.produceLatencyP99Micros));
  const p50 = Math.max(0, ...up.map((b) => b.metrics.produceLatencyP50Micros));
  const totalIn = up.reduce((s, b) => s + b.metrics.messagesInTotal, 0);
  const totalLag = snapshot.lag.reduce((s, r) => s + Math.max(0, r.lag), 0);

  return (
    <main className="shell">
      <Masthead
        controllerId={topology.controllerId}
        term={topology.term}
        aliveCount={up.length}
        total={snapshot.brokers.length}
        lastSweep={lastSweep}
      />

      {downUrls.length > 0 && up.length > 0 && (
        <div className="alert">
          <span className="alert-mark">▲</span>
          {downUrls.length} broker{downUrls.length > 1 ? "s" : ""} unreachable: {downUrls.join(" · ")} — partitions led
          there reject writes until the node returns
        </div>
      )}

      {up.length === 0 ? (
        <div className="panel offline">
          <h2>CLUSTER UNREACHABLE</h2>
          <p>No broker answered. Start the cluster with scripts/start-cluster.sh</p>
        </div>
      ) : (
        <>
          <div className="grid-metrics">
            <MetricCard
              label="Produced"
              value={inRate.toFixed(1)}
              unit="msg/s"
              tone="accent"
              spark={history.map((p) => p.inRate)}
              sparkColor="#ffb454"
            />
            <MetricCard
              label="Consumed"
              value={outRate.toFixed(1)}
              unit="msg/s"
              tone="teal"
              spark={history.map((p) => p.outRate)}
              sparkColor="#4fd6be"
            />
            <MetricCard
              label="Produce latency"
              value={formatMicros(p99)}
              sub={`p99 · p50 ${formatMicros(p50)}`}
            />
            <MetricCard label="Messages stored" value={formatCount(totalIn)} sub="since cluster start" />
            <MetricCard
              label="Total lag"
              value={formatCount(totalLag)}
              unit="msgs"
              tone={totalLag > 1000 ? "accent" : ""}
              sub={`${snapshot.lag.length} group-partitions tracked`}
            />
          </div>

          <div style={{ marginBottom: 24 }}>
            <BrokerHealth brokers={snapshot.brokers} />
          </div>

          <div className="grid-low">
            <ThroughputChart history={history} />
            <LagTable lag={snapshot.lag} />
          </div>

          <div style={{ marginTop: 12 }}>
            <PartitionMap topics={topology.topics} aliveById={aliveById} />
          </div>
        </>
      )}

      <footer className="foot">
        <span>hermes · distributed message broker</span>
        <span>
          poll {POLL_MS / 1000}s · raft term {topology.term}
          {lastSweep ? ` · updated ${lastSweep.toLocaleTimeString()}` : ""}
        </span>
      </footer>
    </main>
  );
}

function Masthead({ controllerId, term, aliveCount, total, lastSweep }) {
  const healthy = total > 0 && aliveCount === total;
  return (
    <div className="masthead">
      <div className="wordmark">
        HERMES<span>▞</span>
      </div>
      <div className="tagline">broker telemetry</div>
      <div className="status">
        <span className={`live ${lastSweep ? (healthy ? "ok" : "warn") : ""}`}>
          <i />
          {lastSweep ? (healthy ? "live" : "degraded") : "connecting"}
        </span>
        <span>
          controller <b>{controllerId < 0 ? "—" : `broker ${controllerId}`}</b>
        </span>
        <span>
          term <b>{term}</b>
        </span>
        <span>
          nodes <b>
            {aliveCount}/{total}
          </b>
        </span>
      </div>
    </div>
  );
}
