const W = 600;
const H = 190;
const PAD = { top: 14, right: 10, bottom: 22, left: 46 };
const POLL_SECONDS = 2;

function coords(history, pick, max) {
  const innerW = W - PAD.left - PAD.right;
  const innerH = H - PAD.top - PAD.bottom;
  const step = innerW / Math.max(1, history.length - 1);
  return history.map((point, i) => [
    PAD.left + i * step,
    PAD.top + innerH - (pick(point) / max) * innerH,
  ]);
}

function linePath(pts) {
  return pts.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
}

function areaPath(pts) {
  if (pts.length < 2) return "";
  const bottom = H - PAD.bottom;
  return `${linePath(pts)} L${pts[pts.length - 1][0].toFixed(1)},${bottom} L${pts[0][0].toFixed(1)},${bottom} Z`;
}

export default function ThroughputChart({ history }) {
  const max = Math.max(10, ...history.map((p) => Math.max(p.inRate, p.outRate))) * 1.15;
  const latest = history[history.length - 1] ?? { inRate: 0, outRate: 0 };
  const inPts = history.length > 1 ? coords(history, (p) => p.inRate, max) : [];
  const outPts = history.length > 1 ? coords(history, (p) => p.outRate, max) : [];
  const windowSeconds = Math.max(1, (history.length - 1) * POLL_SECONDS);

  return (
    <div className="panel chart-wrap">
      <div className="microlabel">Throughput · messages/sec</div>
      <svg
        viewBox={`0 0 ${W} ${H}`}
        preserveAspectRatio="none"
        role="img"
        aria-label={`Cluster throughput over the last ${windowSeconds} seconds: producing ${latest.inRate.toFixed(1)} and consuming ${latest.outRate.toFixed(1)} messages per second`}
      >
        <defs>
          <linearGradient id="fillIn" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#ffb454" stopOpacity="0.22" />
            <stop offset="100%" stopColor="#ffb454" stopOpacity="0" />
          </linearGradient>
          <linearGradient id="fillOut" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#4fd6be" stopOpacity="0.18" />
            <stop offset="100%" stopColor="#4fd6be" stopOpacity="0" />
          </linearGradient>
        </defs>
        {[0.25, 0.5, 0.75, 1].map((g) => {
          const y = PAD.top + (H - PAD.top - PAD.bottom) * (1 - g);
          return (
            <g key={g}>
              <line x1={PAD.left} y1={y} x2={W - PAD.right} y2={y} stroke="#1c2530" strokeWidth="1" />
              <text x={PAD.left - 8} y={y + 3} textAnchor="end" fontSize="9" fill="#5c6773">
                {Math.round(max * g)}
              </text>
            </g>
          );
        })}
        {[0, 0.5, 1].map((f) => (
          <text
            key={f}
            x={PAD.left + (W - PAD.left - PAD.right) * f}
            y={H - 8}
            textAnchor={f === 0 ? "start" : f === 1 ? "end" : "middle"}
            fontSize="9"
            fill="#38414d"
          >
            {f === 1 ? "now" : `-${Math.round(windowSeconds * (1 - f))}s`}
          </text>
        ))}
        {inPts.length > 1 && <path d={areaPath(inPts)} fill="url(#fillIn)" />}
        {outPts.length > 1 && <path d={areaPath(outPts)} fill="url(#fillOut)" />}
        {inPts.length > 1 && (
          <path d={linePath(inPts)} fill="none" stroke="#ffb454" strokeWidth="1.8" />
        )}
        {outPts.length > 1 && (
          <path d={linePath(outPts)} fill="none" stroke="#4fd6be" strokeWidth="1.8" />
        )}
        {inPts.length > 0 && (
          <circle cx={inPts[inPts.length - 1][0]} cy={inPts[inPts.length - 1][1]} r="2.5" fill="#ffb454" />
        )}
        {outPts.length > 0 && (
          <circle cx={outPts[outPts.length - 1][0]} cy={outPts[outPts.length - 1][1]} r="2.5" fill="#4fd6be" />
        )}
      </svg>
      <div className="chart-legend">
        <span>
          <span className="key" style={{ background: "#ffb454" }} />
          produced · {latest.inRate.toFixed(1)}/s
        </span>
        <span>
          <span className="key" style={{ background: "#4fd6be" }} />
          consumed · {latest.outRate.toFixed(1)}/s
        </span>
      </div>
    </div>
  );
}
