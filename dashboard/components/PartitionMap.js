import { brokerColor } from "@/lib/api";

export default function PartitionMap({ topics, aliveById }) {
  return (
    <div className="panel">
      <div className="microlabel">Partition Map · leader / replicas</div>
      {topics.length === 0 ? (
        <div className="empty">no topics yet — produce a message to create one</div>
      ) : (
        topics.map((topic) => {
          const maxEnd = Math.max(1, ...topic.partitions.map((p) => p.endOffset));
          return (
            <div className="topic-block" key={topic.name}>
              <div className="tname">
                {topic.name} <span>· {topic.partitionCount} partitions</span>
              </div>
              <div className="pgrid">
                {topic.partitions.map((p) => {
                  const leaderUp = aliveById?.[p.leader] !== false;
                  return (
                    <div
                      className={`pcell ${leaderUp ? "" : "dead"}`}
                      key={p.partition}
                      style={{ "--cell-color": leaderUp ? brokerColor(p.leader) : "var(--red)" }}
                      title={`partition ${p.partition} · leader broker ${p.leader} · replicas ${p.replicas.join(",")}`}
                    >
                      <div className="pnum">P{p.partition}</div>
                      <div className="pleader">
                        B{p.leader}
                        {leaderUp ? "" : " ✕"}
                      </div>
                      <div className="pmeta">
                        r[{p.replicas.join(",")}] · {p.endOffset < 0 ? "—" : `${p.endOffset} msgs`}
                      </div>
                      <div className="pfill">
                        <span
                          style={{
                            width: `${p.endOffset < 0 ? 0 : Math.round((p.endOffset / maxEnd) * 100)}%`,
                          }}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })
      )}
    </div>
  );
}
