import { Button, Field, Panel, Select, TextInput } from "@ot/ui-kit";
import { useCallback, useEffect, useMemo, useState } from "react";
import { connectorApi } from "../api/connectorApi";
import type { ConnectorStatus, SourceConfig, SparkplugHealth } from "../api/types";
import { useAuthState } from "../auth/AuthContext";
import { useAppToast } from "../toast/ToastContext";

// Sparkplug B source settings (sparkplug_connector/sparkplug/README.md); username, password, topics and TLS stay in the
// config file
const EMPTY_SPARKPLUG = {
  client_id: "",
  primary_host_id: "",
  state_format: "v3",
  offline_after_seconds: "",
  online_after_seconds: "",
  nbirth_enabled: false,
  warehouse_id: "",
  procedures: "",
  schema_prefix: "",
  had_nbirth: false, // the source had edge_node_schemas: an edit sends them even when switched off
};

const text = (v: unknown) => (v === undefined || v === null ? "" : String(v));
const switchedOn = (v: unknown) => !["false", "0", "no", "off", ""].includes(text(v).trim().toLowerCase());
// NBIRTH procedures (catalog.schema.procedure): a list, or text separated by commas
const listed = (v: unknown) =>
  (typeof v === "string" ? v.split(",") : Array.isArray(v) ? v : []).map((x) => text(x).trim()).filter(Boolean);

export function SourcesPage() {
  const toast = useAppToast();
  const { can } = useAuthState();

  const [sources, setSources] = useState<SourceConfig[]>([]);
  const [status, setStatus] = useState<ConnectorStatus | null>(null);
  const [sparkplugHealth, setSparkplugHealth] = useState<SparkplugHealth>({});
  const [sparkplug, setSparkplug] = useState(EMPTY_SPARKPLUG);
  const [loading, setLoading] = useState(false);

  const [selected, setSelected] = useState<Record<string, boolean>>({});

  const [mode, setMode] = useState<"create" | "edit">("create");
  const [originalName, setOriginalName] = useState<string>("");
  const [form, setForm] = useState({
    name: "",
    endpoint: "",
    enabled: true,
    target_table: "",
  });

  const activeMap = useMemo(() => {
    const clients = (status as any)?.metrics?.clients || {};
    return new Set(Object.keys(clients));
  }, [status]);

  const refresh = useCallback(async () => {
    if (!can.read) return;
    setLoading(true);
    try {
      const [srcData, st, sp] = await Promise.all([
        connectorApi.getSources(),
        connectorApi.getStatus(),
        connectorApi.getSparkplugHealth().catch(() => ({})),
      ]);
      const list = Array.isArray((srcData as any)?.sources) ? (srcData as any).sources : Array.isArray(srcData) ? srcData : [];
      setSources(list);
      setStatus(st);
      setSparkplugHealth(sp);
    } catch (e) {
      toast.show(e instanceof Error ? e.message : "Failed to load sources", "bad");
    } finally {
      setLoading(false);
    }
  }, [can.read, toast]);

  useEffect(() => {
    refresh().catch(() => {});
  }, [refresh]);

  const toggleAll = useCallback(
    (v: boolean) => {
      const next: Record<string, boolean> = {};
      for (const s of sources) {
        const n = String((s as any)?.name ?? "");
        if (n) next[n] = v;
      }
      setSelected(next);
    },
    [sources]
  );

  const selectedNames = useMemo(() => Object.entries(selected).filter(([, v]) => v).map(([k]) => k), [selected]);

  const startSelected = useCallback(async () => {
    if (!can.start_stop) return toast.show("Requires start_stop permission", "warn");
    const names = selectedNames;
    if (names.length === 0) return toast.show("No sources selected", "warn");
    for (const n of names) {
      try {
        await connectorApi.startSource(n);
      } catch (e) {
        toast.show(e instanceof Error ? e.message : `Failed to start ${n}`, "warn");
      }
    }
    toast.show("Start requested", "ok");
    await refresh();
  }, [can.start_stop, refresh, selectedNames, toast]);

  const stopSelected = useCallback(async () => {
    if (!can.start_stop) return toast.show("Requires start_stop permission", "warn");
    const names = selectedNames;
    if (names.length === 0) return toast.show("No sources selected", "warn");
    for (const n of names) {
      try {
        await connectorApi.stopSource(n);
      } catch (e) {
        toast.show(e instanceof Error ? e.message : `Failed to stop ${n}`, "warn");
      }
    }
    toast.show("Stop requested", "warn");
    await refresh();
  }, [can.start_stop, refresh, selectedNames, toast]);

  const startBridge = useCallback(async () => {
    if (!can.start_stop) return toast.show("Requires start_stop permission", "warn");
    try {
      const res = await connectorApi.startBridge();
      toast.show(String((res as any)?.message ?? "Bridge start requested"), "ok");
      setTimeout(() => refresh().catch(() => {}), 800);
    } catch (e) {
      toast.show(e instanceof Error ? e.message : "Failed to start bridge", "bad");
    }
  }, [can.start_stop, refresh, toast]);

  const fillFormForEdit = useCallback((s: SourceConfig) => {
    setMode("edit");
    const n = String((s as any)?.name ?? "");
    setOriginalName(n);
    setForm({
      name: n,
      endpoint: String((s as any)?.endpoint ?? ""),
      enabled: Boolean((s as any)?.enabled ?? true),
      target_table: text(s.target_table),
    });
    const nbirth = (s.edge_node_schemas ?? {}) as Record<string, unknown>;
    setSparkplug({
      client_id: text(s.client_id),
      primary_host_id: text(s.primary_host_id),
      state_format: text(s.state_format) || "v3",
      offline_after_seconds: text(s.offline_after_seconds),
      online_after_seconds: text(s.online_after_seconds),
      nbirth_enabled: s.edge_node_schemas ? switchedOn(nbirth.enabled ?? true) : false,
      warehouse_id: text(nbirth.warehouse_id),
      procedures: listed(nbirth.procedures).join(", "),
      schema_prefix: text(nbirth.schema_prefix),
      had_nbirth: Boolean(s.edge_node_schemas),
    });
  }, []);

  const resetForm = useCallback(() => {
    setMode("create");
    setOriginalName("");
    setForm({ name: "", endpoint: "", enabled: true, target_table: "" });
    setSparkplug(EMPTY_SPARKPLUG);
  }, []);

  const submit = useCallback(async () => {
    if (!can.write) return toast.show("Requires write permission", "warn");
    const payload: SourceConfig = {
      name: form.name.trim(),
      protocol: "sparkplug",
      endpoint: form.endpoint.trim(),
      enabled: Boolean(form.enabled),
    };
    // the source's own table (empty: the ZeroBus default); an edit sends it, so clearing it removes it
    if (form.target_table.trim() || mode === "edit") payload.target_table = form.target_table.trim();
    // an edit sends every field shown (empty = cleared); the server keeps the fields the form doesn't show
    const seconds = (v: string) => (v.trim() === "" ? "" : Number(v));
    const fields: Record<string, unknown> = {
      client_id: sparkplug.client_id.trim(),
      primary_host_id: sparkplug.primary_host_id.trim(),
      state_format: sparkplug.state_format,
      offline_after_seconds: seconds(sparkplug.offline_after_seconds),
      online_after_seconds: seconds(sparkplug.online_after_seconds),
    };
    for (const [key, v] of Object.entries(fields)) if (v !== "" || mode === "edit") payload[key] = v;
    if (sparkplug.nbirth_enabled || (mode === "edit" && sparkplug.had_nbirth)) {
      payload.edge_node_schemas = {
        enabled: sparkplug.nbirth_enabled,
        warehouse_id: sparkplug.warehouse_id.trim(),
        procedures: listed(sparkplug.procedures),
        schema_prefix: sparkplug.schema_prefix.trim(),
      };
    }
    try {
      if (mode === "create") {
        const res = await connectorApi.addSource(payload);
        toast.show(String((res as any)?.message ?? "Source added"), "ok");
      } else {
        const res = await connectorApi.updateSource(originalName, payload);
        toast.show(String((res as any)?.message ?? "Source updated"), "ok");
      }
      resetForm();
      await refresh();
    } catch (e) {
      toast.show(e instanceof Error ? e.message : "Save failed", "bad");
    }
  }, [can.write, form, mode, originalName, refresh, resetForm, sparkplug, toast]);

  const deleteSource = useCallback(
    async (name: string) => {
      if (!can.delete) return toast.show("Requires delete permission", "warn");
      if (!window.confirm(`Delete source "${name}"?`)) return;
      try {
        const res = await connectorApi.deleteSource(name);
        toast.show(String((res as any)?.message ?? "Source deleted"), "warn");
        await refresh();
      } catch (e) {
        toast.show(e instanceof Error ? e.message : "Delete failed", "bad");
      }
    },
    [can.delete, refresh, toast]
  );

  const startOne = useCallback(
    async (name: string) => {
      if (!can.start_stop) return toast.show("Requires start_stop permission", "warn");
      try {
        const res = await connectorApi.startSource(name);
        toast.show(String((res as any)?.message ?? "Start requested"), "ok");
        await refresh();
      } catch (e) {
        toast.show(e instanceof Error ? e.message : "Start failed", "bad");
      }
    },
    [can.start_stop, refresh, toast]
  );

  const stopOne = useCallback(
    async (name: string) => {
      if (!can.start_stop) return toast.show("Requires start_stop permission", "warn");
      try {
        const res = await connectorApi.stopSource(name);
        toast.show(String((res as any)?.message ?? "Stop requested"), "warn");
        await refresh();
      } catch (e) {
        toast.show(e instanceof Error ? e.message : "Stop failed", "bad");
      }
    },
    [can.start_stop, refresh, toast]
  );

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <Panel
        title="Sources"
        subtitle="Sparkplug B sources (MQTT servers) and their lifecycle"
        actions={
          <div style={{ display: "flex", gap: 10 }}>
            <Button type="button" onClick={() => refresh()} disabled={loading || !can.read}>
              {loading ? "Refreshing..." : "Refresh"}
            </Button>
          </div>
        }
      >
        <div style={{ display: "flex", gap: 10, flexWrap: "wrap", justifyContent: "flex-end" }}>
          <Button type="button" onClick={() => startBridge()} disabled={!can.start_stop}>
            Start bridge
          </Button>
          <Button variant="primary" type="button" onClick={() => startSelected()} disabled={!can.start_stop}>
            Start selected
          </Button>
          <Button variant="danger" type="button" onClick={() => stopSelected()} disabled={!can.start_stop}>
            Stop selected
          </Button>
        </div>
      </Panel>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1.4fr", gap: 16, alignItems: "start" }}>
        <Panel title={mode === "create" ? "Add source" : "Edit source"} subtitle="Writes to config and updates running bridge">
          <div style={{ display: "grid", gap: 10 }}>
            <Field label="Name">
              <TextInput
                value={form.name}
                onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
                placeholder="plant-mqtt-server"
              />
            </Field>

            <Field label="Endpoint (MQTT server)">
              <TextInput
                value={form.endpoint}
                onChange={(e) => setForm((f) => ({ ...f, endpoint: e.target.value }))}
                placeholder="mqtt://192.168.1.100:1883 (mqtts://...:8883 for TLS)"
              />
            </Field>
            <Field label="Target table">
              <TextInput
                value={form.target_table}
                onChange={(e) => setForm((f) => ({ ...f, target_table: e.target.value }))}
                placeholder="catalog.schema.table (empty: the ZeroBus default table)"
              />
            </Field>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
              <Field label="Client ID">
                <TextInput
                  value={sparkplug.client_id}
                  onChange={(e) => setSparkplug((p) => ({ ...p, client_id: e.target.value }))}
                  placeholder="databricks-sparkplug"
                />
              </Field>
              <Field label="Primary host ID">
                <TextInput
                  value={sparkplug.primary_host_id}
                  onChange={(e) => setSparkplug((p) => ({ ...p, primary_host_id: e.target.value }))}
                  placeholder="empty: a plain subscriber"
                />
              </Field>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
              <Field label="STATE format">
                <Select value={sparkplug.state_format} onChange={(e) => setSparkplug((p) => ({ ...p, state_format: e.target.value }))}>
                  <option value="v3">v3 (spBv1.0/STATE)</option>
                  <option value="v2">v2 (STATE, legacy)</option>
                </Select>
              </Field>
              <Field label="Offline after (s)">
                <TextInput
                  type="number"
                  min={1}
                  value={sparkplug.offline_after_seconds}
                  onChange={(e) => setSparkplug((p) => ({ ...p, offline_after_seconds: e.target.value }))}
                  placeholder="30"
                />
              </Field>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
              <Field label="Online after (s)">
                <TextInput
                  type="number"
                  min={1}
                  value={sparkplug.online_after_seconds}
                  onChange={(e) => setSparkplug((p) => ({ ...p, online_after_seconds: e.target.value }))}
                  placeholder="30"
                />
              </Field>
              <label className="field">
                <div className="field-label">Procedure on NBIRTH</div>
                <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
                  <input
                    type="checkbox"
                    checked={sparkplug.nbirth_enabled}
                    onChange={(e) => setSparkplug((p) => ({ ...p, nbirth_enabled: e.target.checked }))}
                    style={{ width: 18, height: 18 }}
                  />
                  <span className="muted">Call it after each edge node birth</span>
                </div>
              </label>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
              <Field label="SQL warehouse ID">
                <TextInput
                  value={sparkplug.warehouse_id}
                  onChange={(e) => setSparkplug((p) => ({ ...p, warehouse_id: e.target.value }))}
                  placeholder="warehouse id"
                />
              </Field>
              <Field label="Schema prefix">
                <TextInput
                  value={sparkplug.schema_prefix}
                  onChange={(e) => setSparkplug((p) => ({ ...p, schema_prefix: e.target.value }))}
                  placeholder="nb"
                />
              </Field>
            </div>
            <Field label="NBIRTH procedure">
              <TextInput
                value={sparkplug.procedures}
                onChange={(e) => setSparkplug((p) => ({ ...p, procedures: e.target.value }))}
                placeholder="bridge_node_db.stage_db.create_edge_node_schema (comma-separated)"
              />
            </Field>
            <div className="muted">
              catalog.schema.procedure, called as procedure(prefix, group_id, edge_node_id, false) like
              create_edge_node_schema.
            </div>
            <div className="muted">
              Primary host ID: the edges&apos; Primary Host ID (the edges buffer while the connector is OFFLINE);
              empty: a plain subscriber (press Rebirth on the edges after starting it).
            </div>
            <div className="muted">Username, password, topics and TLS: in the configuration file only.</div>

            <label className="field">
              <div className="field-label">Enabled</div>
              <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
                <input
                  type="checkbox"
                  checked={form.enabled}
                  onChange={(e) => setForm((f) => ({ ...f, enabled: e.target.checked }))}
                  style={{ width: 18, height: 18 }}
                />
                <span className="muted">Start source when connector runs</span>
              </div>
            </label>

            <div style={{ display: "flex", gap: 10 }}>
              <Button variant="primary" type="button" onClick={() => submit()} disabled={!can.write}>
                {mode === "create" ? "Add source" : "Save changes"}
              </Button>
              <Button type="button" onClick={() => resetForm()}>
                Reset
              </Button>
            </div>

            <div className="muted">
              Editing a source updates the on-disk config and restarts the source inside the running bridge.
            </div>
          </div>
        </Panel>

        <Panel title="Configured sources" subtitle={`${sources.length} configured`}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 10, marginBottom: 10 }}>
            <label className="muted" style={{ display: "flex", gap: 8, alignItems: "center" }}>
              <input
                type="checkbox"
                checked={sources.length > 0 && selectedNames.length === sources.length}
                onChange={(e) => toggleAll(e.target.checked)}
              />
              Select all
            </label>
            <Button type="button" onClick={() => refresh()} disabled={loading || !can.read}>
              Refresh
            </Button>
          </div>

          <div style={{ border: "1px solid var(--border-panel)", borderRadius: 2, overflow: "hidden" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "var(--font-data)", fontSize: 12 }}>
              <thead>
                <tr style={{ background: "rgba(0,0,0,0.35)" }}>
                  <th style={{ textAlign: "left", padding: 8, width: 44 }} />
                  <th style={{ textAlign: "left", padding: 8 }}>Name</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Primary host</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Endpoint</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Active</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Enabled</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Action</th>
                  <th style={{ textAlign: "right", padding: 8, width: 44 }} />
                </tr>
              </thead>
              <tbody>
                {sources.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="muted" style={{ padding: 10 }}>
                      No sources configured yet.
                    </td>
                  </tr>
                ) : (
                  sources.map((s) => {
                    const name = String((s as any)?.name ?? "");
                    const primaryHost = text((s as any)?.primary_host_id);
                    const endpoint = String((s as any)?.endpoint ?? "");
                    const enabled = (s as any)?.enabled;
                    const active = activeMap.has(name);
                    const sparkplugState = sparkplugHealth[name]?.state;
                    return (
                      <tr key={name} style={{ borderTop: "1px solid rgba(255,255,255,0.03)" }}>
                        <td style={{ padding: 8 }}>
                          <input
                            type="checkbox"
                            checked={Boolean(selected[name])}
                            onChange={(e) => setSelected((m) => ({ ...m, [name]: e.target.checked }))}
                          />
                        </td>
                        <td style={{ padding: 8 }}>
                          <code>{name}</code>
                        </td>
                        <td style={{ padding: 8 }}>
                          {primaryHost ? <code>{primaryHost}</code> : <span className="muted">plain subscriber</span>}
                        </td>
                        <td style={{ padding: 8 }}>
                          <code>{endpoint}</code>
                        </td>
                        <td style={{ padding: 8 }}>
                          {active ? <strong>yes</strong> : <span className="muted">no</span>}
                          {active && sparkplugState ? <span className="muted"> · {sparkplugState}</span> : null}
                        </td>
                        <td style={{ padding: 8 }}>
                          {typeof enabled === "boolean" ? (enabled ? "true" : "false") : <span className="muted">n/a</span>}
                        </td>
                        <td style={{ padding: 8 }}>
                          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                            {active ? (
                              <Button variant="danger" type="button" onClick={() => stopOne(name)} disabled={!can.start_stop}>
                                Stop
                              </Button>
                            ) : (
                              <Button variant="primary" type="button" onClick={() => startOne(name)} disabled={!can.start_stop}>
                                Start
                              </Button>
                            )}
                            <Button type="button" onClick={() => fillFormForEdit(s)} disabled={!can.write}>
                              Edit
                            </Button>
                          </div>
                        </td>
                        <td style={{ padding: 8, textAlign: "right" }}>
                          <Button variant="danger" type="button" onClick={() => deleteSource(name)} disabled={!can.delete}>
                            x
                          </Button>
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </Panel>
      </div>
    </div>
  );
}

