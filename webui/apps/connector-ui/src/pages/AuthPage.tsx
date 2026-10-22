import { Button, KvTable, Panel } from "@ot/ui-kit";
import { useCallback, useEffect, useState } from "react";
import { connectorApi } from "../api/connectorApi";
import type { AuthStatus } from "../api/types";
import { useAuthState } from "../auth/AuthContext";
import { useAppToast } from "../toast/ToastContext";

export function AuthPage() {
  const toast = useAppToast();
  const { can } = useAuthState();
  const [status, setStatus] = useState<AuthStatus | null>(null);
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      setStatus(await connectorApi.getAuthStatus());
    } catch (e) {
      toast.show(e instanceof Error ? e.message : "Failed to load auth status", "bad");
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    refresh().catch(() => {});
  }, [refresh]);

  const logout = useCallback(async () => {
    try {
      const res = await connectorApi.logout();
      toast.show(String((res as any)?.message ?? "Logged out"), "warn");
      // send the user to the login page
      window.location.href = "/static/login.html";
    } catch (e) {
      toast.show(e instanceof Error ? e.message : "Logout failed", "bad");
    }
  }, [toast]);

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <Panel title="Auth" subtitle="Session + permissions (cookie-based)">
        <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
          <a href="/login" style={{ textDecoration: "none" }}>
            <Button variant="primary" type="button">
              Login
            </Button>
          </a>
          <Button variant="danger" type="button" onClick={() => logout()}>
            Logout
          </Button>
          <Button type="button" onClick={() => refresh()} disabled={loading}>
            {loading ? "Refreshing..." : "Refresh"}
          </Button>
        </div>
        <div className="muted" style={{ marginTop: 10 }}>
          Uses <code>/api/auth/status</code>, <code>/api/auth/permissions</code>, and <code>/logout</code>.
        </div>
      </Panel>

      <Panel title="Auth status" subtitle="Raw response">
        <KvTable data={status} emptyText={can.read ? "Loading…" : "Requires read permission"} />
      </Panel>
    </div>
  );
}

