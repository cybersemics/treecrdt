import React from "react";
import { formatContentBytes, SUPPORTED_IMAGE_MIME_TYPES, validateImageContentFile } from "@treecrdt/content";
import { createPortal } from "react-dom";
import { MdAdd, MdImage, MdShuffle, MdTextFields, MdAccountTree } from "react-icons/md";

type MenuLayout = {
  top: number;
  left: number;
  width: number;
  maxHeight: number;
};

type AddMode = "menu" | "text" | "bulk";

export function AddNodeMenu({
  parentId,
  parentLabel,
  variant = "header",
  ready,
  busy,
  canWritePayload,
  canWriteStructure,
  maxNodeCount,
  onAddText,
  onAddImage,
  onAddBulk,
}: {
  parentId: string;
  parentLabel?: string;
  variant?: "header" | "row";
  ready: boolean;
  busy: boolean;
  canWritePayload: boolean;
  canWriteStructure: boolean;
  maxNodeCount: number;
  onAddText: (parentId: string, value: string) => void | Promise<void>;
  onAddImage: (parentId: string, file: File) => void | Promise<void>;
  onAddBulk: (parentId: string, count: number, fanout: number, value: string) => void | Promise<void>;
}) {
  const [mode, setMode] = React.useState<AddMode | null>(null);
  const [layout, setLayout] = React.useState<MenuLayout | null>(null);
  const [textValue, setTextValue] = React.useState("");
  const [bulkValue, setBulkValue] = React.useState("");
  const [bulkCount, setBulkCount] = React.useState(100);
  const [bulkFanout, setBulkFanout] = React.useState(10);
  const [selectedImage, setSelectedImage] = React.useState<File | null>(null);
  const [error, setError] = React.useState<string | null>(null);
  const [randomBusy, setRandomBusy] = React.useState(false);
  const buttonRef = React.useRef<HTMLButtonElement | null>(null);
  const menuRef = React.useRef<HTMLDivElement | null>(null);
  const fileInputRef = React.useRef<HTMLInputElement | null>(null);
  const textInputRef = React.useRef<HTMLInputElement | null>(null);
  const isOpen = mode !== null;
  const disabled = !ready || busy || !canWriteStructure;

  const updateLayout = React.useCallback(() => {
    const button = buttonRef.current;
    if (!button || typeof window === "undefined") return;
    const rect = button.getBoundingClientRect();
    const width = variant === "header" ? 320 : 300;
    const margin = 12;
    const left = Math.min(Math.max(margin, rect.right - width), window.innerWidth - width - margin);
    const top = Math.min(rect.bottom + 8, window.innerHeight - 160);
    setLayout({
      top,
      left,
      width,
      maxHeight: Math.max(180, window.innerHeight - top - margin),
    });
  }, [variant]);

  const close = React.useCallback(() => {
    setMode(null);
    setError(null);
    setSelectedImage(null);
    if (fileInputRef.current) fileInputRef.current.value = "";
  }, []);

  const open = React.useCallback(() => {
    if (disabled) return;
    setMode("menu");
    setError(null);
    updateLayout();
  }, [disabled, updateLayout]);

  React.useEffect(() => {
    if (!isOpen) return;
    updateLayout();
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (target && (buttonRef.current?.contains(target) || menuRef.current?.contains(target))) return;
      close();
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") close();
    };
    window.addEventListener("resize", updateLayout);
    window.addEventListener("scroll", updateLayout, true);
    window.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("resize", updateLayout);
      window.removeEventListener("scroll", updateLayout, true);
      window.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [close, isOpen, updateLayout]);

  React.useEffect(() => {
    if (mode !== "text") return;
    const id = window.requestAnimationFrame(() => textInputRef.current?.focus());
    return () => window.cancelAnimationFrame(id);
  }, [mode]);

  const runCreate = React.useCallback(
    async (create: () => void | Promise<void>) => {
      setError(null);
      try {
        await create();
        setTextValue("");
        setBulkValue("");
        setSelectedImage(null);
        close();
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    },
    [close],
  );

  const submitImage = React.useCallback(
    async (file: File | null) => {
      if (!file) return;
      try {
        validateImageContentFile(file);
        setSelectedImage(file);
        await runCreate(() => onAddImage(parentId, file));
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (fileInputRef.current) fileInputRef.current.value = "";
      }
    },
    [onAddImage, parentId, runCreate],
  );

  const fetchRandomImage = React.useCallback(async () => {
    setRandomBusy(true);
    setError(null);
    try {
      const seed =
        typeof crypto !== "undefined" && "randomUUID" in crypto
          ? crypto.randomUUID()
          : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      const response = await fetch(`https://picsum.photos/seed/treecrdt-${seed}/640/420`, {
        cache: "no-store",
      });
      if (!response.ok) throw new Error(`random image request failed (${response.status})`);
      const blob = await response.blob();
      const file = new File([blob], `random-${seed}.jpg`, { type: blob.type || "image/jpeg" });
      await submitImage(file);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setRandomBusy(false);
    }
  }, [submitImage]);

  const parentContext = parentLabel ? `Under ${parentLabel}` : "Add under selected parent";
  const buttonClass =
    variant === "header"
      ? "relative flex h-9 items-center gap-2 rounded-lg border border-accent bg-accent px-3 text-xs font-semibold text-white shadow-lg shadow-accent/25 transition hover:-translate-y-0.5 hover:bg-accent/90 disabled:translate-y-0 disabled:opacity-50"
      : "relative flex h-8 w-8 items-center justify-center rounded-lg border border-slate-700 bg-slate-800/70 text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50";

  return (
    <>
      <button
        ref={buttonRef}
        type="button"
        className={buttonClass}
        onClick={() => (isOpen ? close() : open())}
        disabled={disabled}
        aria-haspopup="menu"
        aria-expanded={isOpen}
        aria-label={variant === "header" ? "Add" : "Add child"}
        title={variant === "header" ? "Add node" : "Add child"}
      >
        <MdAdd className={variant === "header" ? "text-[18px]" : "text-[20px]"} />
        {variant === "header" ? <span>Add</span> : null}
      </button>
      <input
        ref={fileInputRef}
        type="file"
        accept={SUPPORTED_IMAGE_MIME_TYPES.join(",")}
        className="hidden"
        onChange={(event) => void submitImage(event.target.files?.[0] ?? null)}
      />
      {isOpen && layout && typeof document !== "undefined"
        ? createPortal(
            <div
              ref={menuRef}
              className="fixed z-[180] overflow-auto rounded-2xl border border-slate-700 bg-slate-950/95 p-3 text-xs text-slate-200 shadow-2xl shadow-black/50 backdrop-blur"
              style={{
                top: `${layout.top}px`,
                left: `${layout.left}px`,
                width: `${layout.width}px`,
                maxHeight: `${layout.maxHeight}px`,
              }}
            >
              <div className="mb-2 text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                {parentContext}
              </div>
              {mode === "menu" ? (
                <div className="space-y-1">
                  <MenuAction
                    icon={<MdTextFields />}
                    label="Text node"
                    detail="Create one child with a text payload."
                    disabled={!canWritePayload}
                    onClick={() => setMode("text")}
                  />
                  <MenuAction
                    icon={<MdImage />}
                    label="Image node"
                    detail="Pick a file and create an image child."
                    disabled={!canWritePayload}
                    onClick={() => fileInputRef.current?.click()}
                  />
                  <MenuAction
                    icon={<MdShuffle />}
                    label={randomBusy ? "Fetching random image..." : "Random image"}
                    detail="Download a Picsum JPEG and create an image child."
                    disabled={!canWritePayload || randomBusy}
                    onClick={() => void fetchRandomImage()}
                  />
                  <MenuAction
                    icon={<MdAccountTree />}
                    label="Bulk nodes"
                    detail="Generate a flat list or k-ary tree for stress testing."
                    onClick={() => setMode("bulk")}
                  />
                </div>
              ) : null}
              {mode === "text" ? (
                <form
                  className="space-y-3"
                  onSubmit={(event) => {
                    event.preventDefault();
                    void runCreate(() => onAddText(parentId, textValue));
                  }}
                >
                  <label className="block space-y-1">
                    <span className="text-[11px] font-semibold text-slate-300">Text payload</span>
                    <input
                      ref={textInputRef}
                      value={textValue}
                      onChange={(event) => setTextValue(event.target.value)}
                      placeholder="Node text"
                      className="w-full rounded-lg border border-slate-700 bg-slate-900/80 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/40"
                    />
                  </label>
                  <div className="flex items-center justify-end gap-2">
                    <MenuButton type="button" onClick={() => setMode("menu")}>
                      Back
                    </MenuButton>
                    <MenuButton type="submit" primary>
                      Create text node
                    </MenuButton>
                  </div>
                </form>
              ) : null}
              {mode === "bulk" ? (
                <form
                  className="space-y-3"
                  onSubmit={(event) => {
                    event.preventDefault();
                    const count = Math.max(1, Math.min(maxNodeCount, Math.floor(bulkCount)));
                    const fanout = Math.max(0, Math.floor(bulkFanout));
                    void runCreate(() => onAddBulk(parentId, count, fanout, bulkValue));
                  }}
                >
                  <label className="block space-y-1">
                    <span className="text-[11px] font-semibold text-slate-300">Value prefix</span>
                    <input
                      value={bulkValue}
                      onChange={(event) => setBulkValue(event.target.value)}
                      placeholder="Optional text prefix"
                      className="w-full rounded-lg border border-slate-700 bg-slate-900/80 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/40"
                    />
                  </label>
                  <div className="grid grid-cols-2 gap-2">
                    <label className="block space-y-1">
                      <span className="text-[11px] font-semibold text-slate-300">Node count</span>
                      <input
                        type="number"
                        min={1}
                        max={maxNodeCount}
                        value={bulkCount}
                        onChange={(event) => setBulkCount(Number(event.target.value) || 1)}
                        className="w-full rounded-lg border border-slate-700 bg-slate-900/80 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/40"
                      />
                    </label>
                    <label className="block space-y-1">
                      <span className="text-[11px] font-semibold text-slate-300">Fanout</span>
                      <select
                        value={bulkFanout}
                        onChange={(event) => setBulkFanout(Number(event.target.value) || 0)}
                        className="w-full rounded-lg border border-slate-700 bg-slate-900/80 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/40"
                      >
                        <option value={0}>Flat</option>
                        <option value={2}>2</option>
                        <option value={3}>3</option>
                        <option value={5}>5</option>
                        <option value={10}>10</option>
                        <option value={20}>20</option>
                        <option value={50}>50</option>
                      </select>
                    </label>
                  </div>
                  <div className="flex items-center justify-end gap-2">
                    <MenuButton type="button" onClick={() => setMode("menu")}>
                      Back
                    </MenuButton>
                    <MenuButton type="submit" primary>
                      Create bulk nodes
                    </MenuButton>
                  </div>
                </form>
              ) : null}
              {selectedImage ? (
                <div className="mt-2 truncate text-[11px] text-slate-400" title={selectedImage.name}>
                  {selectedImage.name} · {formatContentBytes(selectedImage.size)}
                </div>
              ) : null}
              {error ? <div className="mt-2 rounded-lg bg-rose-500/10 px-2 py-1.5 font-semibold text-rose-100">{error}</div> : null}
            </div>,
            document.body,
          )
        : null}
    </>
  );
}

function MenuAction({
  icon,
  label,
  detail,
  disabled,
  onClick,
}: {
  icon: React.ReactNode;
  label: string;
  detail: string;
  disabled?: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      className="flex w-full items-center gap-3 rounded-xl px-3 py-2 text-left transition hover:bg-slate-800/80 disabled:opacity-40 disabled:hover:bg-transparent"
      disabled={disabled}
      onClick={onClick}
      role="menuitem"
    >
      <span className="text-[18px] text-accent">{icon}</span>
      <span className="min-w-0">
        <span className="block font-semibold text-white">{label}</span>
        <span className="block text-[11px] text-slate-400">{detail}</span>
      </span>
    </button>
  );
}

function MenuButton({
  type,
  primary,
  onClick,
  children,
}: {
  type: "button" | "submit";
  primary?: boolean;
  onClick?: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type={type}
      onClick={onClick}
      className={
        primary
          ? "rounded-lg bg-accent px-3 py-2 text-xs font-semibold text-white transition hover:bg-accent/90"
          : "rounded-lg border border-slate-700 bg-slate-900/70 px-3 py-2 text-xs font-semibold text-slate-200 transition hover:border-slate-500 hover:text-white"
      }
    >
      {children}
    </button>
  );
}
