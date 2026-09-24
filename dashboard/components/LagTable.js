function badgeClass(lag) {
  if (lag < 0) return "lagbadge";
  if (lag > 1000) return "lagbadge hot";
  if (lag > 100) return "lagbadge warn";
  return "lagbadge";
}

export default function LagTable({ lag }) {
  return (
    <div className="panel">
      <div className="microlabel">Consumer Lag</div>
      {lag.length === 0 ? (
        <div className="empty">no consumer groups registered</div>
      ) : (
        <table className="lag">
          <caption className="sr-only">
            Consumer group lag per partition: committed offset, end offset and remaining messages
          </caption>
          <thead>
            <tr>
              <th>group</th>
              <th className="num">part</th>
              <th className="num">committed</th>
              <th className="num">end</th>
              <th>progress</th>
              <th className="num">lag</th>
            </tr>
          </thead>
          <tbody>
            {lag.map((row) => {
              const done =
                row.endOffset <= 0
                  ? 1
                  : Math.min(1, Math.max(0, row.committed) / row.endOffset);
              return (
                <tr key={`${row.group}-${row.topic}-${row.partition}`}>
                  <td title={`topic ${row.topic} · generation ${row.generation} · ${row.members} member(s)`}>
                    {row.group}
                    <span className="dim"> /{row.topic}</span>
                  </td>
                  <td className="num">{row.partition}</td>
                  <td className="num">{row.committed < 0 ? "—" : row.committed}</td>
                  <td className="num">{row.endOffset < 0 ? "—" : row.endOffset}</td>
                  <td>
                    <div className="lagbar">
                      <span style={{ width: `${Math.round(done * 100)}%` }} />
                    </div>
                  </td>
                  <td className="num">
                    <span className={badgeClass(row.lag)}>{row.lag < 0 ? "?" : row.lag}</span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}
