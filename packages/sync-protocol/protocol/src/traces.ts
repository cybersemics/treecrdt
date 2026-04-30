export type HelloTraceRecord = {
  type: 'sync-hello-trace';
  docId: string;
  stage: string;
  ms: number;
} & Record<string, unknown>;

export type HelloTraceSink = (record: HelloTraceRecord) => void;

const HELLO_TRACE_SINK_KEY = '__TREECRDT_SYNC_HELLO_TRACE_SINK__';

function getHelloTraceSink(): HelloTraceSink | undefined {
  const sink = (globalThis as Record<string, unknown>)[HELLO_TRACE_SINK_KEY];
  return typeof sink === 'function' ? (sink as HelloTraceSink) : undefined;
}

export function installHelloTraceSink(sink: HelloTraceSink): () => void {
  const root = globalThis as Record<string, unknown>;
  const previousSink = getHelloTraceSink();
  const nextSink: HelloTraceSink = (record) => {
    previousSink?.(record);
    sink(record);
  };
  root[HELLO_TRACE_SINK_KEY] = nextSink;
  return () => {
    if (previousSink === undefined) {
      delete root[HELLO_TRACE_SINK_KEY];
    } else {
      root[HELLO_TRACE_SINK_KEY] = previousSink;
    }
  };
}

export function traceHello(
  docId: string,
  startedAt: number,
  stage: string,
  extra: Record<string, unknown> = {},
): void {
  const sink = getHelloTraceSink();
  if (!sink) return;
  const record: HelloTraceRecord = {
    type: 'sync-hello-trace',
    docId,
    stage,
    ms: performance.now() - startedAt,
    ...extra,
  };
  try {
    sink(record);
  } catch {
    // debug tracing must never affect sync behavior
  }
}
