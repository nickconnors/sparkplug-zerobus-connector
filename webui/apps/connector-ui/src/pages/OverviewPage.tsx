import { Button, Panel, StatusPill } from "@ot/ui-kit";
import { useCallback, useEffect, useMemo, useState } from "react";
import { connectorApi } from "../api/connectorApi";
import type { ConnectorStatus, SparkplugHealth } from "../api/types";
import { useAuthState } from "../auth/AuthContext";
import { useAppToast } from "../toast/ToastContext";

export function OverviewPage() {
  const { can } = useAuthState();
  const toast = useAppToast();
  const [status, setStatus] = useState<ConnectorStatus | null>(null);
  const [health, setHealth] = useState<SparkplugHealth>({});
  const [loading, setLoading] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const refresh = useCallback(async () => {
    if (!can.read) {
      toast.show("Requires read permission", "warn");
      return;
    }
    setLoading(true);
    try {
      const [st, sh] = await Promise.all([connectorApi.getStatus(), connectorApi.getSparkplugHealth()]);
      setStatus(st);
      setHealth(sh ?? {});
      setLastUpdated(new Date());
    } catch (e) {
      toast.show(e instanceof Error ? e.message : "Failed to refresh", "bad");
    } finally {
      setLoading(false);
    }
  }, [can.read, toast]);

  useEffect(() => {
    refresh().catch(() => {});
  }, [refresh]);

  const badges = useMemo(() => {
    const st = status ?? {};
    const zerobusConnected =
      (st as any)?.zerobus_connected ?? (st as any)?.zerobus?.connected ?? (st as any)?.zerobusConnected;
    const configured = (st as any)?.configured_sources;
    const active = (st as any)?.active_sources;

    const sourcesText =
      typeof configured === "number" && typeof active === "number"
        ? `Sources: ${configured} configured (${active} active)`
        : typeof configured === "number"
          ? `Sources: ${configured} configured`
          : "Sources: unknown";
    const sourcesKind: "ok" | "warn" | "bad" =
      typeof active === "number" ? (active > 0 ? "ok" : "warn") : typeof configured === "number" ? "ok" : "warn";

    const zbText =
      typeof zerobusConnected === "boolean"
        ? zerobusConnected
          ? "ZeroBus: connected"
          : "ZeroBus: disconnected"
        : "ZeroBus: unknown";
    const zbKind: "ok" | "warn" | "bad" =
      typeof zerobusConnected === "boolean" ? (zerobusConnected ? "ok" : "bad") : "warn";

    return {
      sources: { text: sourcesText, kind: sourcesKind },
      zerobus: { text: zbText, kind: zbKind },
    };
  }, [status]);

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <Panel
        title="Overview"
        subtitle={
          <>
            High-level health + refresh{" "}
            {lastUpdated && <span className="muted">(updated {lastUpdated.toLocaleTimeString()})</span>}
          </>
        }
        actions={
          <Button type="button" onClick={() => refresh()} disabled={loading || !can.read}>
            {loading ? "Refreshing..." : "Refresh"}
          </Button>
        }
      >
        <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
          <StatusPill kind={badges.sources.kind}>{badges.sources.text}</StatusPill>
          <StatusPill kind={badges.zerobus.kind}>{badges.zerobus.text}</StatusPill>
          {Object.entries(health).map(([name, h]) => (
            <StatusPill key={name} kind={h.state === "ONLINE" ? "ok" : "bad"}>
              {name}: {h.primary_host_id ? `primary host ${String(h.state ?? "unknown")}` : h.mqtt_connected ? "subscribed" : "not connected"}
            </StatusPill>
          ))}
        </div>
        <div className="muted" style={{ marginTop: 10 }}>
          Driven by <code>/api/status</code> and <code>/api/sparkplug/health</code> (each running source's state).
        </div>
      </Panel>
    </div>
  );
}

