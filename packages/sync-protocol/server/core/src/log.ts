import pino from 'pino';
import type { DestinationStream, Logger } from 'pino';
import pinoPretty from 'pino-pretty';

/**
 * Sync-server log levels, from most to least verbose.
 * `silent` drops every line, including errors.
 */
export const SYNC_SERVER_LOG_LEVELS = ['debug', 'info', 'warn', 'error', 'silent'] as const;

export type SyncServerLogLevel = (typeof SYNC_SERVER_LOG_LEVELS)[number];

export type SyncServerLogFields = Record<string, unknown>;

/**
 * Leveled logger backed by pino. Calls below the configured level are no-ops,
 * and a failing sink cannot throw into server code.
 */
export type SyncServerLogger = {
  readonly level: SyncServerLogLevel;
  debug(message: string, fields?: SyncServerLogFields): void;
  info(message: string, fields?: SyncServerLogFields): void;
  warn(message: string, fields?: SyncServerLogFields): void;
  error(message: string, fields?: SyncServerLogFields): void;
};

export type SyncServerLoggerOptions = {
  level?: SyncServerLogLevel;
  /** Fields included on every line, such as `{ component: 'sync-server' }`. */
  bindings?: SyncServerLogFields;
  /**
   * Human-readable output instead of pino JSON.
   * The CLI turns this on outside production.
   */
  pretty?: boolean;
  /** Overrides stdout. Tests use this to capture records. */
  destination?: DestinationStream;
};

function isLogLevel(value: string): value is SyncServerLogLevel {
  return (SYNC_SERVER_LOG_LEVELS as readonly string[]).includes(value);
}

function emit(
  logger: Logger,
  method: 'debug' | 'info' | 'warn' | 'error',
  message: string,
  fields?: SyncServerLogFields,
): void {
  try {
    if (fields) logger[method](fields, message);
    else logger[method](message);
  } catch {
    // logging must never affect server behavior
  }
}

function wrap(logger: Logger): SyncServerLogger {
  return {
    get level() {
      return logger.level as SyncServerLogLevel;
    },
    debug: (message, fields) => emit(logger, 'debug', message, fields),
    info: (message, fields) => emit(logger, 'info', message, fields),
    warn: (message, fields) => emit(logger, 'warn', message, fields),
    error: (message, fields) => emit(logger, 'error', message, fields),
  };
}

/**
 * Builds a pino logger for the sync server.
 * The default level is `info`. Pass {@link resolveSyncServerLogLevel} to follow the environment.
 */
export function createSyncServerLogger(opts: SyncServerLoggerOptions = {}): SyncServerLogger {
  const level = opts.level ?? 'info';
  if (!isLogLevel(level)) {
    throw new Error(`invalid sync-server log level: ${String(level)}`);
  }

  const destination =
    opts.destination ??
    (opts.pretty
      ? pinoPretty({
          colorize: Boolean(process.stdout.isTTY),
          sync: true,
          translateTime: 'SYS:standard',
        })
      : undefined);
  const root = pino({ level, timestamp: pino.stdTimeFunctions.isoTime }, destination);
  const logger = opts.bindings ? root.child(opts.bindings) : root;
  return wrap(logger);
}

/** Drops every log line. Used when a caller does not opt into logging. */
export const silentSyncServerLogger: SyncServerLogger = createSyncServerLogger({
  level: 'silent',
});

/**
 * Resolves the sync-server log level.
 * `TREECRDT_LOG_LEVEL` wins. Otherwise `NODE_ENV=production` (and `prod`) uses
 * `info`; development, test, and an unset environment use `debug`.
 */
export function resolveSyncServerLogLevel(
  env: Record<string, string | undefined> = process.env,
): SyncServerLogLevel {
  const explicit = env.TREECRDT_LOG_LEVEL?.trim().toLowerCase();
  if (explicit) {
    if (!isLogLevel(explicit)) {
      throw new Error(
        `TREECRDT_LOG_LEVEL must be one of ${SYNC_SERVER_LOG_LEVELS.join(', ')}; got: ${env.TREECRDT_LOG_LEVEL}`,
      );
    }
    return explicit;
  }

  const nodeEnv = env.NODE_ENV?.trim().toLowerCase() ?? '';
  if (nodeEnv === 'production' || nodeEnv === 'prod') return 'info';
  return 'debug';
}

/**
 * Pretty logs outside production. `TREECRDT_LOG_PRETTY=true|false` overrides that.
 * Production stays on pino's JSON lines unless pretty is forced on.
 */
export function resolveSyncServerLogPretty(
  env: Record<string, string | undefined> = process.env,
): boolean {
  const explicit = env.TREECRDT_LOG_PRETTY?.trim().toLowerCase();
  if (explicit) {
    if (['1', 'true', 'yes', 'on', 'pretty'].includes(explicit)) return true;
    if (['0', 'false', 'no', 'off', 'json'].includes(explicit)) return false;
    throw new Error(
      `TREECRDT_LOG_PRETTY must be a boolean (true/false), got: ${env.TREECRDT_LOG_PRETTY}`,
    );
  }
  const nodeEnv = env.NODE_ENV?.trim().toLowerCase() ?? '';
  return nodeEnv !== 'production' && nodeEnv !== 'prod';
}

export function syncServerErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim().length > 0) return error.message;
  return String(error);
}
