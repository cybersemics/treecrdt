# Contributing

## Pull Requests

See [AGENTS.md](../AGENTS.md) for the shared implementation and review checklist.

- Keep PRs focused. Split unrelated CI, docs, package, and behavior changes when practical.
- Add or update tests for behavior changes.
- Run the relevant local checks before requesting review and report any checks you could not run.

Common checks from the repository root:

```sh
pnpm build
pnpm test
pnpm fmt:check:ts
pnpm fmt:check:rust
```

`pnpm test` runs the browser and native Node suites, not every test in the repository. Run the affected package and Rust tests as appropriate; see the [CI workflow](../.github/workflows/ci.yml) for toolchain setup and additional checks.

## Changesets

CI checks release metadata on pull requests. A changeset is required when a PR changes a release-tracked package and should be included in the next npm release. Ignored packages are listed in [the changeset configuration](../.changeset/config.json).

Add a changeset on the feature branch:

```sh
pnpm changeset
```

Choose the changed package, choose `patch`, `minor`, or `major`, and write a short package-level release note. The changeset text becomes the changelog entry, so it should describe the user-visible package change rather than every commit.

No changeset is needed for changes outside release-tracked packages, such as root docs, CI, or ignored examples.

If a PR changes files inside a release-tracked package (including docs or tests) but should not publish a release, add an empty changeset:

```sh
pnpm changeset --empty
```

## Releases

Merged changesets are collected on `main`. The release workflow opens or updates a release PR that bumps package versions, updates changelogs, removes consumed changeset files, and refreshes the lockfile.

Merging the release PR publishes the changed packages.
