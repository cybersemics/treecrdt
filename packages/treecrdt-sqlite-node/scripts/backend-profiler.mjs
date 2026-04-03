const REGISTRY_KEY = Symbol.for("treecrdt.sync.backendProfiler");

function registry() {
  if (!globalThis[REGISTRY_KEY]) {
    globalThis[REGISTRY_KEY] = new Map();
  }
  return globalThis[REGISTRY_KEY];
}

function emptyMethodStats() {
  return {
    calls: 0,
    errors: 0,
    totalMs: 0,
    maxMs: 0,
    inputItems: 0,
    outputItems: 0,
  };
}

function emptyLabelStats() {
  return {
    listOpRefs: emptyMethodStats(),
    getOpsByOpRefs: emptyMethodStats(),
    applyOps: emptyMethodStats(),
  };
}

function ensureDocStats(docId) {
  const docs = registry();
  let docStats = docs.get(docId);
  if (!docStats) {
    docStats = new Map();
    docs.set(docId, docStats);
  }
  return docStats;
}

function ensureLabelStats(docId, label) {
  const docStats = ensureDocStats(docId);
  let labelStats = docStats.get(label);
  if (!labelStats) {
    labelStats = emptyLabelStats();
    docStats.set(label, labelStats);
  }
  return labelStats;
}

function finalizeMethodStats(stats) {
  return {
    ...stats,
    avgMs: stats.calls > 0 ? stats.totalMs / stats.calls : 0,
    avgInputItems: stats.calls > 0 ? stats.inputItems / stats.calls : 0,
    avgOutputItems: stats.calls > 0 ? stats.outputItems / stats.calls : 0,
  };
}

function record(docId, label, method, durationMs, inputItems, outputItems, ok) {
  const labelStats = ensureLabelStats(docId, label);
  const stats = labelStats[method];
  stats.calls += 1;
  stats.totalMs += durationMs;
  stats.maxMs = Math.max(stats.maxMs, durationMs);
  stats.inputItems += inputItems;
  stats.outputItems += outputItems;
  if (!ok) stats.errors += 1;
}

function countListOpRefsOutput(value) {
  return Array.isArray(value) ? value.length : 0;
}

function countGetOpsByOpRefsOutput(value) {
  return Array.isArray(value) ? value.length : 0;
}

export function resetBackendProfiler(docId) {
  if (typeof docId === "string" && docId.length > 0) {
    registry().delete(docId);
    return;
  }
  registry().clear();
}

export function takeBackendProfilerSnapshot(docId) {
  const docStats = registry().get(docId);
  if (!docStats) return null;

  const labels = {};
  for (const [label, stats] of docStats.entries()) {
    labels[label] = {
      listOpRefs: finalizeMethodStats(stats.listOpRefs),
      getOpsByOpRefs: finalizeMethodStats(stats.getOpsByOpRefs),
      applyOps: finalizeMethodStats(stats.applyOps),
    };
  }
  return { docId, labels };
}

export function wrapBackendWithProfiler(backend, { docId, label }) {
  if (!backend || typeof backend !== "object") return backend;
  if (typeof docId !== "string" || docId.length === 0) return backend;
  if (typeof label !== "string" || label.length === 0) return backend;

  return {
    ...backend,
    listOpRefs: async (filter) => {
      const startedAt = performance.now();
      try {
        const result = await backend.listOpRefs(filter);
        record(docId, label, "listOpRefs", performance.now() - startedAt, 1, countListOpRefsOutput(result), true);
        return result;
      } catch (error) {
        record(docId, label, "listOpRefs", performance.now() - startedAt, 1, 0, false);
        throw error;
      }
    },
    getOpsByOpRefs: async (opRefs) => {
      const inputItems = Array.isArray(opRefs) ? opRefs.length : 0;
      const startedAt = performance.now();
      try {
        const result = await backend.getOpsByOpRefs(opRefs);
        record(
          docId,
          label,
          "getOpsByOpRefs",
          performance.now() - startedAt,
          inputItems,
          countGetOpsByOpRefsOutput(result),
          true
        );
        return result;
      } catch (error) {
        record(docId, label, "getOpsByOpRefs", performance.now() - startedAt, inputItems, 0, false);
        throw error;
      }
    },
    applyOps: async (ops) => {
      const inputItems = Array.isArray(ops) ? ops.length : 0;
      const startedAt = performance.now();
      try {
        const result = await backend.applyOps(ops);
        record(docId, label, "applyOps", performance.now() - startedAt, inputItems, 0, true);
        return result;
      } catch (error) {
        record(docId, label, "applyOps", performance.now() - startedAt, inputItems, 0, false);
        throw error;
      }
    },
  };
}
