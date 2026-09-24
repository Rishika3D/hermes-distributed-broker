import { brokerColor } from "@/lib/api";

function fmt(n) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

function BrokerCard({ broker }) {
  const m = broker.metrics;
  const up = Boolean(m);
  const isController = up && m.role === "LEADER";
  const color = up ? brokerColor(m.brokerId) : "var(--red)";
  return (
    <div className={`panel broker-card ${up ? "" : "down"}`} style={{ "--broker-color": color }}>
      <div className="head">
        <span className={`pulse ${up ? "" : "dead"}`} />
        <span className="id" style={{ color: up ? color : "var(--text-dim)" }}>
          {up ? `BROKER ${m.brokerId}` : "BROKER ?"}
        </span>
        <span className={`role ${isController ? "leader" : ""}`}>
          {up ? (isController ? "★ CONTROLLER" : m.role) : "UNREACHABLE"}
        </span>
      </div>
      <div className="addr">
        {broker.url}
        {up ? <span className="term"> · term {m.term}</span> : null}
      </div>
      {up ? (
        <>
          <div className="stats">
            <span>
              in <b>{m.messagesInPerSec.toFixed(1)}/s</b>
            </span>
            <span>
              out <b>{m.messagesOutPerSec.toFixed(1)}/s</b>
            </span>
            <span>
              p99 <b>{m.produceLatencyP99Micros >= 1000
                ? `${(m.produceLatencyP99Micros / 1000).toFixed(1)}ms`
                : `${m.produceLatencyP99Micros}µs`}</b>
            </span>
          </div>
          <div className="stats faint">
            <span>
              stored <b>{fmt(m.messagesInTotal)}</b>
            </span>
            <span>
              served <b>{fmt(m.messagesOutTotal)}</b>
            </span>
            <span>
              under-repl <b className={m.underReplicatedWrites > 0 ? "warn" : ""}>
                {fmt(m.underReplicatedWrites)}
              </b>
            </span>
            <span>
              up <b>{Math.floor(m.uptimeSeconds / 60)}m</b>
            </span>
          </div>
        </>
      ) : (
        <div className="stats">
          <span>no response from REST API — is the process running?</span>
        </div>
      )}
    </div>
  );
}

export default function BrokerHealth({ brokers }) {
  return (
    <section>
      <div className="microlabel">Broker Health</div>
      <div className="broker-row">
        {brokers.map((broker) => (
          <BrokerCard key={broker.url} broker={broker} />
        ))}
      </div>
    </section>
  );
}
