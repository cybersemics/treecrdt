# Contributing

## Pull Requests

- Keep PRs focused. Split unrelated CI, docs, package, and behavior changes when practical.
- Add or update tests for behavior changes.
- Run the relevant local checks before requesting review.

Common checks:

```sh
pnpm build
pnpm test
pnpm fmt:check:ts
pnpm fmt:check:rust
```

## Changesets

CI checks release metadata on pull requests. A changeset is required when a PR changes a release-tracked package and should be included in the next npm release.

Add a changeset on the feature branch:

```sh
pnpm changeset
```

Choose the changed package, choose `patch`, `minor`, or `major`, and write a short package-level release note. The changeset text becomes the changelog entry, so it should describe the user-visible package change rather than every commit.

No changeset is needed for docs-only, CI-only, example-only, or other non-package changes.

If a PR changes package files but should not publish a release, add an empty changeset:

```sh
pnpm changeset --empty
```

## Releases

Merged changesets are collected on `main`. The release workflow opens or updates a release PR that bumps package versions, updates changelogs, removes consumed changeset files, and refreshes the lockfile.

Merging the release PR publishes the changed packages.
