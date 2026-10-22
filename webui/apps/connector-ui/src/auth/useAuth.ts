import { useEffect, useMemo, useState } from "react";
import { connectorApi } from "../api/connectorApi";
import type { AuthStatus, Permission } from "../api/types";
import { AuthRequiredError } from "../api/client";

const ALL_PERMS: Permission[] = ["read", "write", "configure", "start_stop", "delete", "manage_users"];

export function useAuth() {
  const [authStatus, setAuthStatus] = useState<AuthStatus | null>(null);
  // Start permissive so the UI stays responsive even if auth endpoints hang.
  // Backend still enforces auth/permissions and will return 401/403 as needed.
  const [permissions, setPermissions] = useState<string[]>(ALL_PERMS);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function run() {
      try {
        // Avoid UI wedging forever if auth status endpoint is slow/hung.
        const st = await Promise.race<AuthStatus>([
          connectorApi.getAuthStatus(),
          new Promise<AuthStatus>((_, reject) => setTimeout(() => reject(new Error("Auth status timeout")), 4000)),
        ]);
        if (cancelled) return;
        setAuthStatus(st);

        // If auth is disabled, grant all permissions.
        if (!st.auth_enabled) {
          setPermissions(ALL_PERMS);
          setLoading(false);
          return;
        }

        // If auth enabled but not authenticated, redirect to login UI
        if (!st.authenticated) {
          window.location.href = "/static/login.html";
          return;
        }

        const p = await connectorApi.getPermissions();
        if (cancelled) return;
        setPermissions((p.permissions ?? []) as string[]);
        setLoading(false);
      } catch (e) {
        // If the backend says auth is required, redirect to the login.
        if (e instanceof AuthRequiredError) {
          window.location.href = "/static/login.html";
          return;
        }
        // Otherwise fail open (show UI, backend will still protect endpoints).
        if (cancelled) return;
        setAuthStatus({ auth_enabled: false });
        setPermissions(ALL_PERMS);
        setLoading(false);
      }
    }
    run();
    return () => {
      cancelled = true;
    };
  }, []);

  const can = useMemo(() => {
    const has = (perm: string) => permissions.includes(perm);
    return {
      read: has("read"),
      write: has("write"),
      configure: has("configure"),
      start_stop: has("start_stop"),
      delete: has("delete"),
      manage_users: has("manage_users"),
    };
  }, [permissions]);

  return { authStatus, permissions, can, loading };
}

