import { EventEmitter } from 'node:events';

import {
  resolveSyncServerLogLevel,
  resolveSyncServerLogPretty,
  type SyncServerLogFields,
  type SyncServerLogLevel,
  type SyncServerLogger,
} from '@treecrdt/sync-server-core';
import pino, { type DestinationStream, type Logger } from 'pino';

export type CliSyncServerLoggerOptions = {
  level?: SyncServerLogLevel;
  pretty?: boolean;
  destination?: DestinationStream;
  bindings?: SyncServerLogFields;
};

function guardStream(stream: DestinationStream): void {
  const emitter = stream as unknown as EventEmitter;
  if (typeof emitter.on !== 'function') return;
  emitter.on('error', () => {
    // A failed log sink must not take down the process.
  });
}

function wrap(logger: Logger, level: SyncServerLogLevel): SyncServerLogger {
  const emit = (
    method: 'debug' | 'info' | 'warn' | 'error',
    message: string,
    fields?: SyncServerLogFields,
  ) => {
    try {
      if (fields) logger[method](fields, message);
      else logger[method](message);
    } catch {
      // Logging must not take down the process.
    }
  };
  return {
    level,
    debug: (message, fields) => emit('debug', message, fields),
    info: (message, fields) => emit('info', message, fields),
    warn: (message, fields) => emit('warn', message, fields),
    error: (message, fields) => emit('error', message, fields),
  };
}

/**
 * Pino logger for the CLI. Pretty output loads `pino-pretty` only when requested,
 * so a production install that omits that package still writes JSON.
 */
export async function createCliSyncServerLogger(
  opts: CliSyncServerLoggerOptions = {},
): Promise<SyncServerLogger> {
  const level = opts.level ?? resolveSyncServerLogLevel();
  const pretty = opts.pretty ?? resolveSyncServerLogPretty();
  let destination = opts.destination;
  if (!destination && pretty) {
    try {
      const prettyMod = await import('pino-pretty');
      destination = prettyMod.default({
        colorize: Boolean(process.stdout.isTTY),
        sync: true,
        translateTime: 'SYS:standard',
      });
    } catch {
      // Production installs omit the pretty printer. JSON is the fallback.
    }
  }
  if (!destination) destination = pino.destination({ sync: true });
  guardStream(destination);
  const root = pino({ level, timestamp: pino.stdTimeFunctions.isoTime }, destination);
  return wrap(root.child(opts.bindings ?? { component: 'sync-server' }), level);
}
