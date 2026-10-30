# 0001. Import the engine with git subtree

- Status: accepted
- Date: 2026-10-30
- Milestone: M0

## Context

The matching engine already exists in its own repo (`cpp_exchange`, 25 commits). It needs to live in `engine/` of this repo, and its history shows how it was built, which is useful in interviews.

## Options

1. **git subtree**: copies the engine's commits into this repo under `engine/`. One repo, full history, clones normally. The commands (`git subtree add/pull`) are less familiar.
2. **Plain copy**: simplest. Loses all history; the engine looks like it appeared in one commit.
3. **git submodule**: keeps the repos separate and links a commit. Every clone and CI job needs `--recursive`; easy to end up with a detached or stale submodule.

## Decision

git subtree (option 1): keeps history without making cloning or CI harder.

## Consequences

- `engine/` is now developed here. If the old repo changes, pull it with `git subtree pull --prefix=engine <path-or-url> main`.
- The original `cpp_exchange` repo stays as-is; new engine work happens in vortex.
