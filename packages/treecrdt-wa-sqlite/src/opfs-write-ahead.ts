export type OpfsWriteAheadExecutor = <T>(
  sql: string,
  fn: () => Promise<T> | T,
  opts?: { allowTransactionControlBatch?: boolean },
) => Promise<T>;

type TransactionControl =
  | { type: 'savepoint'; name: string }
  | { type: 'release'; name: string }
  | { type: 'rollback-to'; name: string }
  | { type: 'begin'; mode: 'deferred' | 'immediate' | 'exclusive' }
  | { type: 'finish' };

type ParsedTransactionControls = {
  controls: TransactionControl[];
};

const SAVEPOINT_NAME_PATTERN =
  '(?:"(?:[^"]|"")*"|\'(?:[^\']|\'\')*\'|`(?:[^`]|``)*`|\\[(?:[^\\]]|\\]\\])*\\]|[a-z_][a-z0-9_$]*)';

function splitSqlStatements(sql: string): string[] {
  const statements: string[] = [];
  let statement = '';
  let quote: '"' | "'" | '`' | '[' | null = null;

  const pushStatement = () => {
    const normalized = statement.trim();
    if (normalized) statements.push(normalized);
    statement = '';
  };

  for (let i = 0; i < sql.length; i += 1) {
    const ch = sql[i]!;
    const next = sql[i + 1];

    if (quote) {
      statement += ch;
      const close = quote === '[' ? ']' : quote;
      if (ch === close) {
        if (next === close) {
          statement += next;
          i += 1;
        } else {
          quote = null;
        }
      }
      continue;
    }

    if (ch === '-' && next === '-') {
      statement += ' ';
      i += 2;
      while (i < sql.length && sql[i] !== '\n' && sql[i] !== '\r') i += 1;
      statement += ' ';
      continue;
    }
    if (ch === '/' && next === '*') {
      statement += ' ';
      i += 2;
      while (i < sql.length && !(sql[i] === '*' && sql[i + 1] === '/')) i += 1;
      if (i >= sql.length) {
        throw new Error('OPFS write-ahead mode cannot safely parse an unterminated SQL comment');
      }
      i += 1;
      statement += ' ';
      continue;
    }
    if (ch === '"' || ch === "'" || ch === '`' || ch === '[') {
      quote = ch;
      statement += ch;
      continue;
    }
    if (ch === ';') {
      pushStatement();
      continue;
    }
    statement += ch;
  }

  if (quote) {
    throw new Error('OPFS write-ahead mode cannot safely parse unterminated quoted SQL');
  }
  pushStatement();
  return statements;
}

function normalizeSavepointName(name: string): string {
  const quote = name[0];
  if (quote === '"' || quote === "'" || quote === '`') {
    return name.slice(1, -1).split(`${quote}${quote}`).join(quote).toLowerCase();
  }
  if (quote === '[') return name.slice(1, -1).split(']]').join(']').toLowerCase();
  return name.toLowerCase();
}

function matchSavepointName(statement: string, pattern: string): string | null {
  const match = new RegExp(`^${pattern}\\s+(${SAVEPOINT_NAME_PATTERN})$`, 'i').exec(statement);
  return match ? normalizeSavepointName(match[1]!) : null;
}

function parseTransactionControl(statement: string): {
  control: TransactionControl | null;
  transactionSyntax: boolean;
} {
  const firstKeyword = /^([a-z]+)/i.exec(statement)?.[1]?.toLowerCase();
  const transactionSyntax =
    firstKeyword === 'savepoint' ||
    firstKeyword === 'release' ||
    firstKeyword === 'rollback' ||
    firstKeyword === 'commit' ||
    firstKeyword === 'end' ||
    firstKeyword === 'begin';
  if (!transactionSyntax) return { control: null, transactionSyntax: false };

  const savepoint = matchSavepointName(statement, 'savepoint');
  if (savepoint !== null)
    return { control: { type: 'savepoint', name: savepoint }, transactionSyntax };

  const release = matchSavepointName(statement, 'release(?:\\s+savepoint)?');
  if (release !== null) return { control: { type: 'release', name: release }, transactionSyntax };

  const rollbackTo = matchSavepointName(
    statement,
    'rollback(?:\\s+transaction)?\\s+to(?:\\s+savepoint)?',
  );
  if (rollbackTo !== null) {
    return { control: { type: 'rollback-to', name: rollbackTo }, transactionSyntax };
  }

  if (/^(?:commit|end)(?:\s+transaction)?$/i.test(statement)) {
    return { control: { type: 'finish' }, transactionSyntax };
  }
  if (/^rollback(?:\s+transaction)?$/i.test(statement)) {
    return { control: { type: 'finish' }, transactionSyntax };
  }
  const begin = /^begin(?:\s+(deferred|immediate|exclusive))?(?:\s+transaction)?$/i.exec(statement);
  if (begin) {
    const mode = (begin[1]?.toLowerCase() ?? 'deferred') as 'deferred' | 'immediate' | 'exclusive';
    return { control: { type: 'begin', mode }, transactionSyntax };
  }

  return { control: null, transactionSyntax };
}

function transactionControls(
  sql: string,
  allowTransactionControlBatch: boolean,
): ParsedTransactionControls | null {
  const statements = splitSqlStatements(sql);
  const parsed = statements.map(parseTransactionControl);
  if (!parsed.some((value) => value.transactionSyntax)) return null;
  if (parsed.some((value) => !value.control)) {
    throw new Error(
      'OPFS write-ahead mode cannot safely track unsupported or mixed transaction-control SQL',
    );
  }
  if (statements.length > 1 && !allowTransactionControlBatch) {
    throw new Error(
      'OPFS write-ahead mode only supports transaction-control batches through runner.exec()',
    );
  }

  const controls = parsed.map((value) => value.control!);
  if (controls.length > 1) {
    if (controls.some((control) => control.type === 'begin')) {
      throw new Error('OPFS write-ahead mode cannot safely track a batched BEGIN');
    }
    for (let i = 0; i < controls.length - 1; i += 1) {
      if (controls[i]!.type === 'finish') {
        throw new Error(
          'OPFS write-ahead mode cannot safely track SQL after a batched COMMIT or ROLLBACK',
        );
      }
      if (controls[i]!.type === 'release') {
        throw new Error('OPFS write-ahead mode cannot safely track SQL after a batched RELEASE');
      }
    }
  }
  return { controls };
}

export function treecrdtSqlRequiresWriteTransaction(sql: string): boolean {
  const normalized = sql.trim().toLowerCase();
  return (
    normalized.startsWith('select treecrdt_append_') ||
    normalized.startsWith('select treecrdt_ensure_materialized') ||
    normalized.startsWith('select treecrdt_local_') ||
    normalized.startsWith('select treecrdt_set_doc_id')
  );
}

/**
 * Wrap TreeCRDT writes for OPFSWriteAheadVFS without taking ownership of a
 * transaction that was opened by the caller.
 *
 * A top-level SQLite SAVEPOINT starts a deferred transaction, which this VFS
 * cannot upgrade to a write transaction. When a savepoint starts in
 * autocommit mode, the executor therefore owns an outer BEGIN IMMEDIATE and
 * finishes it only when that savepoint is released. Nested/caller-owned
 * write transactions are left to their owner, while caller BEGIN DEFERRED is
 * rejected before it can acquire an incompatible read lock.
 */
export function createOpfsWriteAheadExecutor(transaction: {
  exec: (sql: string) => Promise<void> | void;
  getAutocommit: () => Promise<boolean> | boolean;
}): OpfsWriteAheadExecutor {
  const ownedSavepoints: string[] = [];
  let callerTransactionMode: 'immediate' | 'exclusive' | null = null;
  let queue: Promise<void> = Promise.resolve();

  const rollbackOwned = async () => {
    try {
      await transaction.exec('ROLLBACK');
    } catch {
      // Preserve the original write or commit error.
    }
  };

  const run = async <T>(
    sql: string,
    fn: () => Promise<T> | T,
    opts: { allowTransactionControlBatch?: boolean } = {},
  ): Promise<T> => {
    const parsed = transactionControls(sql, opts.allowTransactionControlBatch === true);
    if (parsed) {
      const initialOwnedSavepoints = [...ownedSavepoints];
      const initialCallerTransactionMode = callerTransactionMode;
      const startsOwnedTransaction =
        initialOwnedSavepoints.length === 0 &&
        parsed.controls[0]?.type === 'savepoint' &&
        (await transaction.getAutocommit());
      const nextOwnedSavepoints = [...initialOwnedSavepoints];
      let nextCallerTransactionMode = initialCallerTransactionMode;
      let tracksOwnedTransaction = nextOwnedSavepoints.length > 0 || startsOwnedTransaction;
      let commitOwnedTransaction = false;

      for (const control of parsed.controls) {
        if (control.type === 'begin') {
          if (control.mode === 'deferred') {
            throw new Error(
              'OPFS write-ahead mode requires BEGIN IMMEDIATE or BEGIN EXCLUSIVE; BEGIN DEFERRED is unsafe',
            );
          }
          nextCallerTransactionMode = control.mode;
          continue;
        }
        if (control.type === 'savepoint') {
          if (tracksOwnedTransaction) nextOwnedSavepoints.push(control.name);
          continue;
        }
        if (control.type === 'rollback-to') {
          const index = nextOwnedSavepoints.lastIndexOf(control.name);
          if (index >= 0) nextOwnedSavepoints.length = index + 1;
          continue;
        }
        if (control.type === 'release') {
          const index = nextOwnedSavepoints.lastIndexOf(control.name);
          if (index < 0) continue;
          if (index === 0) {
            nextOwnedSavepoints.length = 0;
            tracksOwnedTransaction = false;
            commitOwnedTransaction = true;
          } else {
            nextOwnedSavepoints.length = index;
          }
          continue;
        }
        if (control.type === 'finish') {
          nextOwnedSavepoints.length = 0;
          nextCallerTransactionMode = null;
          tracksOwnedTransaction = false;
          commitOwnedTransaction = false;
        }
      }

      if (startsOwnedTransaction) await transaction.exec('BEGIN IMMEDIATE');
      try {
        const result = await fn();
        if (commitOwnedTransaction) await transaction.exec('COMMIT');
        ownedSavepoints.splice(0, ownedSavepoints.length, ...nextOwnedSavepoints);
        callerTransactionMode = nextCallerTransactionMode;
        return result;
      } catch (err) {
        const batchMayHaveChangedOwnedState =
          parsed.controls.length > 1 && initialOwnedSavepoints.length > 0;
        if (startsOwnedTransaction || commitOwnedTransaction || batchMayHaveChangedOwnedState) {
          ownedSavepoints.length = 0;
          await rollbackOwned();
        }
        if (parsed.controls.length > 1 && initialCallerTransactionMode !== null) {
          callerTransactionMode = null;
        }
        throw err;
      }
    }

    if (!treecrdtSqlRequiresWriteTransaction(sql)) return await fn();

    // An existing transaction (including the auth savepoint above) owns its
    // own commit/rollback. BEGIN here would be both incorrect and rejected by
    // SQLite as a nested transaction.
    if (ownedSavepoints.length > 0) return await fn();
    if (!(await transaction.getAutocommit())) {
      if (callerTransactionMode) return await fn();
      throw new Error(
        'OPFS write-ahead mode cannot safely execute a TreeCRDT SELECT write in an untracked transaction; use BEGIN IMMEDIATE or BEGIN EXCLUSIVE through runner.exec()',
      );
    }

    await transaction.exec('BEGIN IMMEDIATE');
    try {
      const result = await fn();
      await transaction.exec('COMMIT');
      return result;
    } catch (err) {
      await rollbackOwned();
      throw err;
    }
  };

  return <T>(
    sql: string,
    fn: () => Promise<T> | T,
    opts?: { allowTransactionControlBatch?: boolean },
  ): Promise<T> => {
    const result = queue.then(() => run(sql, fn, opts));
    queue = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  };
}
