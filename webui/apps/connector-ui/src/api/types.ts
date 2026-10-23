export type Permission =
  | "read"
  | "write"
  | "configure"
  | "start_stop"
  | "delete"
  | "manage_users";

export type AuthStatus = {
  auth_enabled: boolean;
  authenticated?: boolean;
  user?: unknown;
};

export type PermissionsResponse = {
  permissions?: Permission[] | string[];
};

export type ConnectorStatus = Record<string, unknown>;
export type ConnectorMetrics = Record<string, unknown>;

export type SourceConfig = Record<string, unknown> & { name?: string; protocol?: string };

export type ZeroBusConfig = {
  enabled?: boolean;
  workspace_host?: string;
  zerobus_endpoint?: string;
  default_target?: { catalog?: string; schema?: string; table?: string; workspace_host?: string };
  auth?: { client_id?: string; client_secret?: string };
  [k: string]: unknown;
};

// GET /api/sparkplug/health: per Sparkplug source, its state (ONLINE/OFFLINE) and counters
export type SparkplugHealth = Record<string, { state?: string; [k: string]: unknown }>;


// GET /api/sparkplug/edge-nodes: per Sparkplug source, its edge nodes and devices, online or offline since when
export type EdgeNodeDevice = {
  device_id: string;
  online: boolean;
  since: number | null;
  since_iso: string | null;
  changed_by: string | null;
};
export type EdgeNode = {
  group_id: string;
  edge_node_id: string;
  online: boolean;
  since: number | null;
  since_iso: string | null;
  changed_by: string | null;
  bdseq: number | null;
  devices: EdgeNodeDevice[];
};
export type EdgeNodes = Record<string, EdgeNode[]>;
