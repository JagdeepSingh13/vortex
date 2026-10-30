# 4. Git subtree and project history

## The goal

The engine already lived in its own repo, `cpp_exchange`, with 25 commits showing how it was built step by step. We wanted it inside `vortex/engine/` **without losing that history**, because "here is how I built the order book, commit by commit" is strong interview evidence. The formal decision is in [ADR 0001](../decisions/0001-import-engine-with-git-subtree.md).

## Three ways to put one repo inside another

| | Plain copy | Submodule | Subtree (chosen) |
|---|---|---|---|
| How | Copy the files, commit | Store a *link* to a commit of the other repo | Copy the files *and* merge the other repo's commits in |
| History of the engine | Lost; it appears in one commit | Stays in the other repo | **Kept, inside vortex** |
| `git clone` | Normal | Needs `--recursive`, or the folder is empty | Normal |
| CI | Simple | Must fetch submodules too | Simple |
| Common pain | None, but history is gone | Forgetting to update, detached HEADs, "why is engine/ empty?" | The command is less familiar |

## What the subtree command did

```bash
git subtree add --prefix=engine ../cpp_exchange main
```

1. **Fetched** all commits of `cpp_exchange`'s `main` branch into vortex.
2. **Created a merge commit** that joins two histories: vortex's (one commit so far) and the engine's (25 commits), with every engine file placed under `engine/`.

```
vortex history:      ● chore: initial skeleton ──────────────┐
                                                             ● feat(engine): import ... (merge)
cpp_exchange:        ●──●──●── ... ──● readme update ────────┘   files moved under engine/
                     (25 commits, original hashes)                          │
                                                                            ● build: container, Makefile, docs
                                                                            ● merge: M0 into main
```

Try it:

```bash
git log --oneline --graph | head -40   # see the two histories joining
git log --oneline -- engine/           # commits touching the engine
git log --follow -- engine/src/matching/order_book.h   # one file's history
```

The engine commits keep their **original hashes** (for example `3d4a5aa readme update`). They are the same objects as in the `cpp_exchange` repo on GitHub.

## Commit identity: why the history was rebuilt once

A commit hash is computed from **everything** in the commit: files, parent hashes, author, date and the exact message bytes. Change one character of a message and you get a new hash, and so does every commit after it.

During M0 the setup commits first had a co-author line, then were rewritten so that all commits are in your name only. The first attempt (`git filter-branch` with a message filter) also changed whitespace in the old engine messages, so all 25 engine commits got new hashes, breaking the link to `cpp_exchange`. The fix was to rebuild only the three new commits on top of the untouched engine history. The lesson: **rewriting history is easy to overdo; check hashes before and after.**

Rewriting is only acceptable before anyone else has the commits (before pushing). After pushing, rewriting means force-pushing and breaking everyone's clones.

## Branch per milestone

```
main ──●────────────────────────●  merge: M0
        \                      /
m0-bootstrap ●──●──●──────────
```

- Each milestone gets a branch (`m0-bootstrap`, `m1-contracts`, ...).
- Work happens there in small commits with conventional messages: `feat(engine): ...`, `docs: ...`, `build: ...`.
- When the acceptance criteria in [ROADMAP.md](../ROADMAP.md) pass, it is merged into `main` with `--no-ff` ("no fast-forward"). That keeps a visible merge commit, so the history shows where each milestone started and ended.

## If the old repo changes

Engine work now happens in vortex. If you ever change `cpp_exchange` and want those changes here:

```bash
git subtree pull --prefix=engine ../cpp_exchange main
```

## Check yourself

1. Why is a submodule awkward for CI and for new clones?
2. What exactly does `git subtree add` create in the history?
3. Why does changing one commit message change the hashes of all later commits?
4. Why is rewriting history fine before a push but risky after?
5. What does `--no-ff` add to the history, and why do we want it?

## Go deeper

- Git internals: blobs, trees, commits and how hashes are formed ("Pro Git", chapter 10)
- `git log --graph`, `git reflog` (how the bad rewrite was undone)
- Conventional Commits specification
