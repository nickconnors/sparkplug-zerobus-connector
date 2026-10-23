import { Button, Panel, StatusPill } from "@ot/ui-kit";
import { useCallback, useEffect, useState } from "react";
import { connectorApi } from "../api/connectorApi";
import type { EdgeNodes } from "../api/types";
import { useAuthState } from "../auth/AuthContext";
import { useAppToast } from "../toast/ToastContext";

const th = { textAlign: "left" as const, padding: 8 };
const td = { padding: 8 };

function since(iso: string | null | undefined, by: string | null | undefined) {
  if (!iso) return <span className="muted">-</span>;
  return (
    <>
      {new Date(iso).toLocaleString()} <span className="muted">({by})</span>
    </>
  );
}

export function EdgeNodesPage() {
  const toast = useAppToast();
  const { can } = useAuthState();
  const [data, setData] = useState<EdgeNodes>({});
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    if (!can.read) return;
    setLoading(true);
    try {
      setData((await connectorApi.getEdgeNodes()) ?? {});
    } catch (e) {
      toast.show(e instanceof Error ? e.message : "Failed to load the edge nodes", "bad");
    } finally {
      setLoading(false);
    }
  }, [can.read, toast]);

  useEffect(() => {
    refresh().catch(() => {});
    const t = window.setInterval(() => refresh().catch(() => {}), 5000);
    return () => window.clearInterval(t);
  }, [refresh]);

  const sources = Object.entries(data);
  return (
    <div style={{ display: "grid", gap: 16 }}>
      <Panel
        title="Edge nodes"
        subtitle="Each running source's edge nodes and devices: online or offline, and since when"
        actions={
          <Button type="button" onClick={() => refresh()} disabled={loading || !can.read}>
            {loading ? "Refreshing..." : "Refresh"}
          </Button>
        }
      >
        {sources.length === 0 && <div className="muted">No running Sparkplug source.</div>}
        {sources.map(([source, nodes]) => (
          <div key={source} style={{ marginBottom: 16 }}>
            <div style={{ marginBottom: 6 }}>
              Source <code>{source}</code>
            </div>
            <div style={{ border: "1px solid var(--border-panel)", borderRadius: 2, overflow: "hidden" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "var(--font-data)", fontSize: 12 }}>
                <thead>
                  <tr style={{ background: "rgba(0,0,0,0.35)" }}>
                    <th style={th}>Group</th>
                    <th style={th}>Edge node / device</th>
                    <th style={th}>State</th>
                    <th style={th}>Since</th>
                    <th style={th}>bdSeq</th>
                  </tr>
                </thead>
                <tbody>
                  {nodes.length === 0 ? (
                    <tr>
                      <td colSpan={5} className="muted" style={td}>
                        No edge node seen yet.
                      </td>
                    </tr>
                  ) : (
                    nodes.flatMap((n) => [
                      <tr key={`${n.group_id}/${n.edge_node_id}`} style={{ borderTop: "1px solid rgba(255,255,255,0.06)" }}>
                        <td style={td}>
                          <code>{n.group_id}</code>
                        </td>
                        <td style={td}>
                          <code>{n.edge_node_id}</code>
                        </td>
                        <td style={td}>
                          <StatusPill kind={n.online ? "ok" : "bad"}>{n.online ? "online" : "offline"}</StatusPill>
                        </td>
                        <td style={td}>{since(n.since_iso, n.changed_by)}</td>
                        <td style={td}>{n.bdseq ?? <span className="muted">-</span>}</td>
                      </tr>,
                      ...n.devices.map((d) => (
                        <tr key={`${n.group_id}/${n.edge_node_id}/${d.device_id}`}>
                          <td style={td} />
                          <td style={{ ...td, paddingLeft: 24 }}>
                            <code>{d.device_id}</code>
                          </td>
                          <td style={td}>
                            <StatusPill kind={d.online ? "ok" : "bad"}>{d.online ? "online" : "offline"}</StatusPill>
                          </td>
                          <td style={td}>{since(d.since_iso, d.changed_by)}</td>
                          <td style={td} />
                        </tr>
                      )),
                    ])
                  )}
                </tbody>
              </table>
            </div>
          </div>
        ))}
        <div className="muted" style={{ marginTop: 10 }}>
          Driven by <code>/api/sparkplug/edge-nodes</code>: the sequencing's state (births bring edge nodes and devices
          online at their own timestamps; an NDEATH takes an edge node and its devices offline when it is received, a
          DDEATH a device at its own timestamp). Metric values aren't kept here: whether they are stale shows in the
          tables downstream.
        </div>
      </Panel>
    </div>
  );
}
