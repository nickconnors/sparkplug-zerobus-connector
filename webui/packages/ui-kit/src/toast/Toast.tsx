import { useCallback, useEffect, useState } from "react";

export type ToastKind = "ok" | "warn" | "bad";

export function useToast() {
  const [toast, setToast] = useState<{ message: string; kind: ToastKind } | null>(null);

  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => setToast(null), 2600);
    return () => clearTimeout(t);
  }, [toast]);

  // stable functions: pages depend on them (a new one per render re-ran their effects at every toast)
  const show = useCallback((message: string, kind: ToastKind = "ok") => setToast({ message, kind }), []);
  const clear = useCallback(() => setToast(null), []);
  return { toast, show, clear };
}

export function Toast(props: { toast: { message: string; kind: ToastKind } | null }) {
  if (!props.toast) return <div id="toast" className="toast" />;
  return (
    <div id="toast" className={`toast show ${props.toast.kind}`.trim()}>
      {props.toast.message}
    </div>
  );
}

