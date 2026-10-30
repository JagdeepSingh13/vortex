# Interview notes

One entry per completed milestone, newest last. Keep each entry short enough to say out loud in two minutes.

Format:

```
## MN — Title (YYYY-MM-DD)
- What I built: one or two sentences.
- Why it is designed this way: the key decision(s), with ADR links.
- Trade-off I can defend: what I gave up and why that was acceptable.
- Numbers: only measured figures, with machine and conditions. "None yet" is fine.
- Likely questions: 2-3 questions an interviewer could ask, with short answers.
- Study further: concepts to read up on.
```

## M0 — Repository bootstrap (2026-10-30)
- What I built: a monorepo for the platform, with my existing C++ matching engine imported into `engine/` and a one-command build (`make engine && make test`) that runs inside a pinned Linux container.
- Why it is designed this way: git subtree keeps the engine's history in one repo ([ADR 0001](decisions/0001-import-engine-with-git-subtree.md)); a container build makes results independent of my Windows machine and avoids MinGW pain with librdkafka later ([ADR 0002](decisions/0002-build-engine-in-linux-container.md)).
- Trade-off I can defend: builds need Docker and are slower the first time; in exchange anyone can reproduce them from a clean clone.
- Numbers: none yet.
- Likely questions:
  - *Why a monorepo?* The services share event schemas and symbol config; one repo keeps them versioned together.
  - *Subtree vs submodule?* Submodules pin a commit in another repo and complicate cloning and CI; subtree copies the history in.
  - *Why pin the image by digest?* A tag like `24.04` can move; a digest cannot, so the build is repeatable.
- Study further: git subtree internals, Docker image digests and layer caching, CMake build types (Release vs Debug).
