# Working agreements

Follow [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) for checks and release metadata.

## Code Review Rules

- Prefer simple, explainable solutions. Additional complexity should provide a
  meaningful benefit without weakening correctness or safety.
- Look for existing implementations before adding code. Reuse or generalize
  genuinely shared behavior; avoid speculative abstractions.
- Inline single-use helpers when that improves readability. Keep helpers that
  name a meaningful concept or simplify complex control flow.
- Put shared behavioral guarantees in existing conformance suites when practical.
  Keep implementation-specific regression tests where needed. Remove overlapping
  tests or assertions only when distinct coverage is preserved.
- Test relevant races and failure paths explicitly for async, persistence, and
  multi-party changes. Prefer deterministic tests. Supplement browser/runtime
  changes with playground checks across multiple tabs, reconnect, close/reopen,
  and refresh where relevant. Report verification gaps; manual checks do not
  replace regression tests.
- Keep documentation focused on contracts, constraints, and rationale that code
  alone does not communicate. Preserve concise cross-runtime encoding contracts;
  avoid restating implementation details.

## Before handoff

- Review the complete PR diff line by line, including helpers, assertions, and
  documentation. Check whether each addition is needed and whether variable and
  function names are clear and consistent.
- Make a separate second review pass over the updated diff. Fix substantiated
  findings, rerun affected checks, and re-review substantive fixes. A review with
  no new actionable findings is a valid outcome. For high-risk changes without an
  independent review, recommend a fresh-context review before merge.
