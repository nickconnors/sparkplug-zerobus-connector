import { Suspense, lazy, useEffect, useMemo, useState } from "react";
import { AppShell, StatusPill, Toast, useToast } from "@ot/ui-kit";
import { ToastProvider } from "./toast/ToastContext";
import { useAuth } from "./auth/useAuth";
import { AuthProvider } from "./auth/AuthContext";
import { NavProvider } from "./nav/NavContext";

const OverviewPage = lazy(() => import("./pages/OverviewPage").then((m) => ({ default: m.OverviewPage })));
const SourcesPage = lazy(() => import("./pages/SourcesPage").then((m) => ({ default: m.SourcesPage })));
const EdgeNodesPage = lazy(() => import("./pages/EdgeNodesPage").then((m) => ({ default: m.EdgeNodesPage })));
const ZeroBusPage = lazy(() => import("./pages/ZeroBusPage").then((m) => ({ default: m.ZeroBusPage })));
const StatusPage = lazy(() => import("./pages/StatusPage").then((m) => ({ default: m.StatusPage })));
const MetricsPage = lazy(() => import("./pages/MetricsPage").then((m) => ({ default: m.MetricsPage })));
const AuthPage = lazy(() => import("./pages/AuthPage").then((m) => ({ default: m.AuthPage })));

type NavKey =
  | "overview"
  | "sources"
  | "edge-nodes"
  | "zerobus"
  | "status"
  | "metrics"
  | "auth";

function loadNavKey(): NavKey {
  const v = window.localStorage.getItem("connector-ui.nav");
  const allowed: NavKey[] = ["overview", "sources", "edge-nodes", "zerobus", "status", "metrics", "auth"];
  return (allowed.includes(v as NavKey) ? (v as NavKey) : "overview");
}

function App() {
  const [active, setActive] = useState<NavKey>(() => loadNavKey());
  const toastApi = useToast();
  const toastCtx = useMemo(() => ({ show: toastApi.show }), [toastApi.show]);
  const auth = useAuth();

  useEffect(() => {
    window.localStorage.setItem("connector-ui.nav", active);
  }, [active]);

  const nav = useMemo(
    () => [
      { key: "overview", label: "Overview" },
      { key: "sources", label: "Sources" },
      { key: "edge-nodes", label: "Edge nodes" },
      { key: "zerobus", label: "ZeroBus" },
      { key: "status", label: "Status" },
      { key: "metrics", label: "Metrics" },
      { key: "auth", label: "Auth" },
    ],
    []
  );

  return (
    <>
      <ToastProvider api={toastCtx}>
        <AuthProvider value={auth}>
          <NavProvider api={{ go: (k) => setActive(k as NavKey) }}>
          <AppShell
        title="Sparkplug Zerobus Connector"
        subtitle="Sparkplug B to Databricks"
        nav={nav}
        activeKey={active}
        onNavigate={(k) => setActive(k as NavKey)}
        topRight={
          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            <StatusPill kind={auth.loading ? "warn" : "ok"}>
              {auth.loading ? "Auth: loading" : auth.authStatus?.auth_enabled ? "Auth: enabled" : "Auth: disabled"}
            </StatusPill>
          </div>
        }
      >
        <Suspense fallback={<div className="muted">Loading…</div>}>
          {active === "overview" && <OverviewPage />}
          {active === "sources" && <SourcesPage />}
          {active === "edge-nodes" && <EdgeNodesPage />}
          {active === "zerobus" && <ZeroBusPage />}
          {active === "status" && <StatusPage />}
          {active === "metrics" && <MetricsPage />}
          {active === "auth" && <AuthPage />}
        </Suspense>
      </AppShell>
        <Toast toast={toastApi.toast} />
          </NavProvider>
        </AuthProvider>
      </ToastProvider>
    </>
  );
}

export default App;
