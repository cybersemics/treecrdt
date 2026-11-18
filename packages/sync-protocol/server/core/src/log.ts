/**
 * Sync-server log levels, from most to least verbose.
 * `silent` drops every line, including errors.
 */
export const SYNC_SERVER_LOG_LEVELS = ['debug', 'info', 'warn', 'error', 'silent'] as const;

export type SyncServerLogLevel = (typeof SYNC_SERVER_LOG_LEVELS)[number];

export type SyncServerLogFields = Record<string, unknown>;

/** Leveled logger. Calls below the configured level are the implementation's concern. */
export type SyncServerLogger = {
  readonly level: SyncServerLogLevel;
  debug(message: string, fields?: SyncServerLogFields): void;
  info(message: string, fields?: SyncServerLogFields): void;
  warn(message: string, fields?: SyncServerLogFields): void;
  error(message: string, fields?: SyncServerLogFields): void;
};

function isLogLevel(value: string): value is SyncServerLogLevel {
  return (SYNC_SERVER_LOG_LEVELS as readonly string[]).includes(value);
}

const noop = () => {};

/** Drops every log line. Used when a caller does not opt into logging. */
export const silentSyncServerLogger: SyncServerLogger = {
  level: 'silent',
  debug: noop,
  info: noop,
  warn: noop,
  error: noop,
};

/**
 * Resolves the sync-server log level.
 * `TREECRDT_LOG_LEVEL` wins. Otherwise the level is `info`, including when
 * `NODE_ENV` is unset.
 */
export function resolveSyncServerLogLevel(
  env: Record<string, string | undefined> = process.env,
): SyncServerLogLevel {
  const explicit = env.TREECRDT_LOG_LEVEL?.trim().toLowerCase();
  if (!explicit) return 'info';
  if (!isLogLevel(explicit)) {
    throw new Error(
      `TREECRDT_LOG_LEVEL must be one of ${SYNC_SERVER_LOG_LEVELS.join(', ')}; got: ${env.TREECRDT_LOG_LEVEL}`,
    );
  }
  return explicit;
}

/**
 * Pretty logs are opt-in. `TREECRDT_LOG_PRETTY=true` enables them; otherwise
 * the CLI writes JSON, including when `NODE_ENV` is unset.
 */
export function resolveSyncServerLogPretty(
  env: Record<string, string | undefined> = process.env,
): boolean {
  const explicit = env.TREECRDT_LOG_PRETTY?.trim().toLowerCase();
  if (!explicit) return false;
  if (['1', 'true', 'yes', 'on', 'pretty'].includes(explicit)) return true;
  if (['0', 'false', 'no', 'off', 'json'].includes(explicit)) return false;
  throw new Error(
    `TREECRDT_LOG_PRETTY must be a boolean (true/false), got: ${env.TREECRDT_LOG_PRETTY}`,
  );
}

export function syncServerErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim().length > 0) return error.message;
  return String(error);
}
