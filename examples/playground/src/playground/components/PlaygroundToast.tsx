import type { Dispatch, SetStateAction } from "react";

export type ToastKind = "success" | "info" | "error";
export type ToastAction = "sync" | "details";
export type ToastState = {
  kind: ToastKind;
  title: string;
  message?: string;
  actions?: ToastAction[];
  durationMs?: number;
};

export type PlaygroundToastProps = {
  toast: ToastState | null;
  setToast: Dispatch<SetStateAction<ToastState | null>>;
  onSync: () => void;
  canSync: boolean;
  onDetails: () => void;
};

export function PlaygroundToast(props: PlaygroundToastProps) {
  const { toast, setToast, onSync, canSync, onDetails } = props;
  if (!toast) return null;

  return (
    <div
      className={`fixed bottom-4 right-4 z-50 w-[min(420px,calc(100vw-2rem))] rounded-xl border px-4 py-3 shadow-lg shadow-black/40 ring-1 ${
        toast.kind === "success"
          ? "border-emerald-400/50 bg-emerald-500/10 ring-emerald-500/10"
          : toast.kind === "error"
            ? "border-rose-400/50 bg-rose-500/10 ring-rose-500/10"
            : "border-slate-700/70 bg-slate-900/70 ring-slate-800/60"
      }`}
      role="status"
      aria-live="polite"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="truncate text-sm font-semibold text-white">{toast.title}</div>
          {toast.message ? <div className="mt-1 text-xs text-slate-200">{toast.message}</div> : null}
        </div>
        <button
          type="button"
          className="rounded-md border border-slate-700 bg-slate-900/60 px-2 py-1 text-xs font-semibold text-slate-200 transition hover:border-slate-500 hover:text-white"
          onClick={() => setToast(null)}
          aria-label="Dismiss notification"
          title="Dismiss"
        >
          ×
        </button>
      </div>
      {toast.actions && toast.actions.length > 0 && (
        <div className="mt-3 flex flex-wrap items-center gap-2">
          {toast.actions.includes("sync") && (
            <button
              type="button"
              className="rounded-lg bg-accent px-3 py-1.5 text-xs font-semibold text-white shadow-lg shadow-accent/30 transition hover:-translate-y-0.5 hover:bg-accent/90 disabled:opacity-50"
              onClick={() => {
                setToast(null);
                onSync();
              }}
              disabled={!canSync}
              title="Sync now"
            >
              Sync now
            </button>
          )}
          {toast.actions.includes("details") && (
            <button
              type="button"
              className="rounded-lg border border-slate-700 bg-slate-900/60 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white"
              onClick={() => {
                setToast(null);
                onDetails();
              }}
              title="Open Sharing & Auth panel"
            >
              Details
            </button>
          )}
        </div>
      )}
    </div>
  );
}

