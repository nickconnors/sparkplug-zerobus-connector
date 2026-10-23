import { apiJson, apiText } from "./client";
import type {
  AuthStatus,
  ConnectorMetrics,
  ConnectorStatus,
  EdgeNodes,
  PermissionsResponse,
  SourceConfig,
  SparkplugHealth,
  ZeroBusConfig,
} from "./types";

export const connectorApi = {
  // auth
  getAuthStatus: () => apiJson<AuthStatus>("/api/auth/status"),
  getPermissions: () => apiJson<PermissionsResponse>("/api/auth/permissions"),
  logout: () => apiJson<{ status?: string; message?: string }>("/logout", { method: "POST", body: "{}" }),

  // status / metrics
  getStatus: () => apiJson<ConnectorStatus>("/api/status"),
  getMetrics: () => apiJson<ConnectorMetrics>("/api/metrics"),

  // sources
  getSources: () => apiJson<{ sources?: SourceConfig[] } | SourceConfig[]>("/api/sources"),
  addSource: (payload: SourceConfig) =>
    apiJson<Record<string, unknown>>("/api/sources", { method: "POST", body: JSON.stringify(payload) }),
  updateSource: (name: string, payload: SourceConfig) =>
    apiJson<Record<string, unknown>>(`/api/sources/${encodeURIComponent(name)}`, {
      method: "PUT",
      body: JSON.stringify(payload),
    }),
  deleteSource: (name: string) =>
    apiJson<Record<string, unknown>>(`/api/sources/${encodeURIComponent(name)}`, { method: "DELETE" }),
  startSource: (name: string) =>
    apiJson<Record<string, unknown>>(`/api/sources/${encodeURIComponent(name)}/start`, { method: "POST", body: "{}" }),
  stopSource: (name: string) =>
    apiJson<Record<string, unknown>>(`/api/sources/${encodeURIComponent(name)}/stop`, { method: "POST", body: "{}" }),

  // bridge
  startBridge: () => apiJson<Record<string, unknown>>("/api/bridge/start", { method: "POST", body: "{}" }),
  stopBridge: () => apiJson<Record<string, unknown>>("/api/bridge/stop", { method: "POST", body: "{}" }),

  // zerobus
  getZeroBusConfig: () => apiJson<ZeroBusConfig>("/api/zerobus/config"),
  saveZeroBusConfig: (cfg: ZeroBusConfig) =>
    apiJson<Record<string, unknown>>("/api/zerobus/config", { method: "POST", body: JSON.stringify(cfg) }),
  startZeroBus: () => apiJson<Record<string, unknown>>("/api/zerobus/start", { method: "POST", body: "{}" }),
  stopZeroBus: () => apiJson<Record<string, unknown>>("/api/zerobus/stop", { method: "POST", body: "{}" }),
  zeroBusDiagnostics: (deep: boolean) => apiJson<Record<string, unknown>>(`/api/zerobus/diagnostics?deep=${deep ? "true" : "false"}`),

  // sparkplug
  getSparkplugHealth: () => apiJson<SparkplugHealth>("/api/sparkplug/health"),
  getEdgeNodes: () => apiJson<EdgeNodes>("/api/sparkplug/edge-nodes"),

  // health (useful for smoke tests / boot)
  getHealth: () => apiText("/health"),
};

