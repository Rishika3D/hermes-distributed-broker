function Sparkline({ data, color }) {
  if (!data || data.length < 2) return null;
  const W = 120;
  const H = 34;
  const max = Math.max(...data, 1);
  const step = W / (data.length - 1);
  const points = data
    .map((v, i) => `${(i * step).toFixed(1)},${(H - (v / max) * (H - 3) - 1).toFixed(1)}`)
    .join(" ");
  return (
    <svg className="spark" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" aria-hidden="true">
      <polyline points={points} fill="none" stroke={color} strokeWidth="1.5" />
    </svg>
  );
}

export default function MetricCard({ label, value, unit, sub, tone, spark, sparkColor }) {
  return (
    <div className="panel metric">
      <div className="microlabel">{label}</div>
      <div className={`value ${tone ?? ""}`}>
        {value}
        {unit ? <span className="unit">{unit}</span> : null}
      </div>
      {sub ? <div className="sub">{sub}</div> : null}
      <Sparkline data={spark} color={sparkColor ?? "#5c6773"} />
    </div>
  );
}
