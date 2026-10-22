import { Button, Field, Panel, TextInput } from "@ot/ui-kit";
import { useCallback, useEffect, useMemo, useState } from "react";
import { connectorApi } from "../api/connectorApi";
import type { ZeroBusConfig } from "../api/types";
import { useAuthState } from "../auth/AuthContext";
import { useAppToast } from "../toast/ToastContext";

export function ZeroBusPage() {
  const toast = useAppToast();
  const { can } = useAuthState();

  const [loading, setLoading] = useState(false);
  const [diagLoading, setDiagLoading] = useState(false);
  const [config, setConfig] = useState<ZeroBusConfig | null>(null);
  const [diag, setDiag] = useState<string>('Click "Diagnostics" to run checks.');

  const [enabled, setEnabled] = useState(false);
  const [workspaceHost, setWorkspaceHost] = useState("");
  const [zerobusEndpoint, setZerobusEndpoint] = useState("");
  const [tableFqn, setTableFqn] = useState("");
  const [clientId, setClientId] = useState("");
  const [clientSecret, setClientSecret] = useState("");


  function parseFqn(fqn: string): { catalog: string; schema: string; table: string } | null {
    const parts = (fqn || "")
      .split(".")
      .map((p) => p.trim())
      .filter(Boolean);
    if (parts.length !== 3) return null;
    return { catalog: parts[0], schema: parts[1], table: parts[2] };
  }

  const hasStoredSecret = useMemo(() => config?.auth?.client_secret === "***", [config]);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const cfg = await connectorApi.getZeroBusConfig();
      setConfig(cfg);

      setEnabled(Boolean(cfg?.enabled));
      setWorkspaceHost(String(cfg?.workspace_host ?? cfg?.default_target?.workspace_host ?? ""));
      setZerobusEndpoint(String(cfg?.zerobus_endpoint ?? ""));

      const catalog = String(cfg?.default_target?.catalog ?? "");
      const schema = String(cfg?.default_target?.schema ?? "");
      const table = String(cfg?.default_target?.table ?? "");
      setTableFqn([catalog, schema, table].filter(Boolean).join("."));

      setClientId(String(cfg?.auth?.client_id ?? ""));
      setClientSecret("");

    } catch (e) {
      toast.show(e instanceof Error ? e.message : "Failed to load ZeroBus config", "bad");
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    load().catch(() => {});
  }, [load]);

  const save = useCallback(async () => {
    if (!can.configure) return toast.show("Requires configure permission", "warn");
    const parsed = parseFqn(tableFqn.trim());
    if (!parsed) return toast.show("Table must be catalog.schema.table", "bad");

    const payload: any = {
      enabled,
      workspace_host: workspaceHost.trim(),
      zerobus_endpoint: zerobusEndpoint.trim(),
      default_target: {
        workspace_host: workspaceHost.trim(),
        catalog: parsed.catalog,
        schema: parsed.schema,
        table: parsed.table,
      },
      auth: {
        client_id: clientId.trim(),
      },
    };
    if (clientSecret.trim()) payload.auth.client_secret = clientSecret.trim();

    try {
      const res = await connectorApi.saveZeroBusConfig(payload);
      const warning = (res as any)?.warning;
      toast.show(warning ? String(warning) : "ZeroBus config saved", warning ? "warn" : "ok");
      await load();
    } catch (e) {
      toast.show(e instanceof Error ? e.message : "Save failed", "bad");
    }
  }, [
    can.configure,
    clientId,
    clientSecret,
    enabled,
    load,
    tableFqn,
    toast,
    workspaceHost,
    zerobusEndpoint,
  ]);

  const diagnostics = useCallback(
    async (deep: boolean) => {
      setDiagLoading(true);
      try {
        setDiag(deep ? "Running deep diagnostics..." : "Running diagnostics...");
        const data = await connectorApi.zeroBusDiagnostics(deep);
        setDiag(JSON.stringify(data, null, 2));
        const ok = (data as any)?.checks?.deep_stream_create_ok;
        if (ok === false) toast.show("ZeroBus diagnostics: issues found", "warn");
        else toast.show("ZeroBus diagnostics: ok", "ok");
      } catch (e) {
        const msg = e instanceof Error ? e.message : "Diagnostics failed";
        setDiag(msg);
        toast.show(msg, "bad");
      } finally {
        setDiagLoading(false);
      }
    },
    [toast]
  );

  const startBridge = useCallback(async () => {
    if (!can.start_stop) return toast.show("Requires start_stop permission", "warn");
    try {
      setDiag("Requesting: start bridge...");
      const res = await connectorApi.startBridge();
      setDiag(String((res as any)?.message ?? "Bridge start requested"));
      toast.show(String((res as any)?.message ?? "Bridge start requested"), "ok");
    } catch (e) {
      setDiag(e instanceof Error ? e.message : "Bridge start failed");
      toast.show(e instanceof Error ? e.message : "Bridge start failed", "bad");
    }
  }, [can.start_stop, toast]);

  const stopBridge = useCallback(async () => {
    if (!can.start_stop) return toast.show("Requires start_stop permission", "warn");
    try {
      setDiag("Requesting: stop bridge...");
      const res = await connectorApi.stopBridge();
      setDiag(String((res as any)?.message ?? "Bridge stop requested"));
      toast.show(String((res as any)?.message ?? "Bridge stop requested"), "warn");
    } catch (e) {
      setDiag(e instanceof Error ? e.message : "Bridge stop failed");
      toast.show(e instanceof Error ? e.message : "Bridge stop failed", "bad");
    }
  }, [can.start_stop, toast]);

  const startZb = useCallback(async () => {
    if (!can.start_stop) return toast.show("Requires start_stop permission", "warn");
    try {
      setDiag("Requesting: start ZeroBus stream...");
      const res = await connectorApi.startZeroBus();
      setDiag(String((res as any)?.message ?? "ZeroBus start requested"));
      toast.show(String((res as any)?.message ?? "ZeroBus start requested"), "ok");
    } catch (e) {
      setDiag(e instanceof Error ? e.message : "ZeroBus start failed");
      toast.show(e instanceof Error ? e.message : "ZeroBus start failed", "bad");
    }
  }, [can.start_stop, toast]);

  const stopZb = useCallback(async () => {
    if (!can.start_stop) return toast.show("Requires start_stop permission", "warn");
    try {
      setDiag("Requesting: stop ZeroBus stream...");
      const res = await connectorApi.stopZeroBus();
      setDiag(String((res as any)?.message ?? "ZeroBus stop requested"));
      toast.show(String((res as any)?.message ?? "ZeroBus stop requested"), "warn");
    } catch (e) {
      setDiag(e instanceof Error ? e.message : "ZeroBus stop failed");
      toast.show(e instanceof Error ? e.message : "ZeroBus stop failed", "bad");
    }
  }, [can.start_stop, toast]);

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <Panel
        title="ZeroBus"
        subtitle="Target configuration + diagnostics"
        actions={
          <div style={{ display: "flex", gap: 10 }}>
            <Button type="button" onClick={() => load()} disabled={loading || !can.read}>
              {loading ? "Loading..." : "Reload"}
            </Button>
            <Button type="button" onClick={() => diagnostics(false)} disabled={diagLoading || !can.read}>
              {diagLoading ? "Running..." : "Diagnostics"}
            </Button>
            <Button variant="primary" type="button" onClick={() => save()} disabled={!can.configure}>
              Save
            </Button>
          </div>
        }
      >
        <div style={{ display: "grid", gap: 10 }}>
          <Field label="Workspace host">
            <TextInput value={workspaceHost} onChange={(e) => setWorkspaceHost(e.target.value)} placeholder="https://adb-..." />
          </Field>
          <Field label="ZeroBus endpoint">
            <TextInput
              value={zerobusEndpoint}
              onChange={(e) => setZerobusEndpoint(e.target.value)}
              placeholder="<workspaceId>.zerobus.<region>.cloud.databricks.com"
            />
          </Field>
          <Field label="Target table (catalog.schema.table)">
            <TextInput value={tableFqn} onChange={(e) => setTableFqn(e.target.value)} placeholder="main.iot_data.sensor_readings" />
          </Field>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
            <Field label="Client ID">
              <TextInput value={clientId} onChange={(e) => setClientId(e.target.value)} placeholder="<oauth-client-id>" />
            </Field>
            <Field
              label="Client secret"
              hint={hasStoredSecret ? "A secret is already stored (leave blank to keep it)." : "No secret stored yet."}
            >
              <TextInput
                value={clientSecret}
                onChange={(e) => setClientSecret(e.target.value)}
                type="password"
                placeholder="(leave blank to keep existing)"
              />
            </Field>
          </div>

          <label className="field">
            <div className="field-label">Enabled</div>
            <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
              <input type="checkbox" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} style={{ width: 18, height: 18 }} />
              <span className="muted">Allow streaming when started</span>
            </div>
          </label>

        </div>
      </Panel>

      <Panel title="Streaming control" subtitle="Start/stop bridge and ZeroBus without changing contracts">
        <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
          <Button variant="primary" type="button" onClick={() => startBridge()}>
            Start bridge
          </Button>
          <Button variant="danger" type="button" onClick={() => stopBridge()}>
            Stop bridge
          </Button>
          <Button variant="primary" type="button" onClick={() => startZb()}>
            Start ZeroBus
          </Button>
          <Button variant="danger" type="button" onClick={() => stopZb()}>
            Stop ZeroBus
          </Button>
          <Button type="button" onClick={() => diagnostics(true)} disabled={diagLoading}>
            Deep diagnostics
          </Button>
        </div>
      </Panel>

      <Panel title="ZeroBus diagnostics" subtitle="JSON output (terminal-ish)">
        <pre className="kvs" style={{ margin: 0 }}>
          <code style={{ color: "var(--text-primary)" }}>{diag}</code>
        </pre>
      </Panel>
    </div>
  );
}

