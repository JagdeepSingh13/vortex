# 1. Docker and the build container

## The problem it solves

The engine was written on Windows with MinGW g++. That works for now, but:

- From M5 the engine needs **librdkafka** (the Kafka client library), which is painful to build with MinGW.
- Everything else in the project (Kafka, Spark, Redis, Airflow) runs in **Linux** containers. The engine should run on the same kind of OS.
- "It builds on my laptop" is not enough. Anyone cloning the repo, including a CI server, should get **the same compiler and the same result**.

## Image vs container

Two words that are easy to mix up:

|          | Image                                     | Container                                                    |
| -------- | ----------------------------------------- | ------------------------------------------------------------ |
| What     | A read-only template: a snapshot of files | A running (or stopped) instance of an image                  |
| Analogy  | A class                                   | An object                                                    |
| Ours     | `vortex/engine-build:m0`                  | Created by each `docker run`, deleted when it exits (`--rm`) |
| Lifetime | Stays on disk until you delete it         | Seconds to hours                                             |

One image, many containers. `make engine` and `make run-sim` each start their own container from the same image.

## What is inside our image

Defined in [infra/docker/engine-build.Dockerfile](../../infra/docker/engine-build.Dockerfile):

```dockerfile
FROM ubuntu:24.04@sha256:fe81...      # 1. base: a minimal Ubuntu filesystem
RUN apt-get install build-essential cmake ninja-build   # 2. tools
WORKDIR /src                           # 3. default folder
```

| Layer              | Contents                                                                | Why                                            |
| ------------------ | ----------------------------------------------------------------------- | ---------------------------------------------- |
| Ubuntu 24.04       | `bash`, `libc`, `apt`, core utilities. **No kernel.**                   | A familiar, long-term-supported Linux userland |
| `build-essential`  | GCC/G++ 13.3, the linker (`ld`), `make`, C/C++ standard library headers | To compile C++20                               |
| `cmake` 3.28       | Reads `engine/CMakeLists.txt`, generates build instructions             | The engine's build system                      |
| `ninja-build` 1.11 | Executes those instructions quickly, rebuilding only what changed       | Fast incremental builds                        |
| `WORKDIR /src`     | Commands start in `/src`                                                | Our project is mounted there                   |

Total size: about 465 MB. **Not in the image:** your source code and the compiled programs. Those stay on your Windows disk (see "bind mounts" below).

### Layers and the build cache

Each instruction in a Dockerfile produces a **layer**: a set of file changes stacked on the previous one. Docker caches layers. If the Dockerfile has not changed, `docker build` reuses them, which is why you saw:

```
=> CACHED [2/3] RUN apt-get update && apt-get install ...
```

and the build took under a second. Change one line, and that layer plus every layer after it is rebuilt.

### Why pin by digest (`@sha256:...`)

`ubuntu:24.04` is a **tag**, a movable label. Ubuntu republishes it with security updates, so the same tag can mean different files next month. `@sha256:fe81...` is a **digest**, a hash of the exact image contents. It can never change. Pinning the digest means a build today and a build in a year start from identical files. That is the "pin exact versions everywhere" rule from `CLAUDE.md`.

The trade-off: we do not get Ubuntu security updates automatically; we must bump the digest deliberately. For a build toolbox that never faces the internet, that is fine.

## Container vs virtual machine

```
Virtual machine                         Container
┌─────────────────────┐                 ┌─────────────────────┐
│ your app            │                 │ your app            │
│ libraries           │                 │ libraries (Ubuntu)  │
│ guest OS + KERNEL   │                 └──────────┬──────────┘
├─────────────────────┤                            │ shares
│ hypervisor          │                 ┌──────────▼──────────┐
└─────────────────────┘                 │ host Linux KERNEL   │
                                        └─────────────────────┘
```

- A **VM** emulates a whole computer and boots its own kernel. Heavy (GBs, seconds to minutes to start).
- A **container** is just a normal process that the Linux kernel isolates: it gets its own view of files (namespaces), and limits on CPU and memory (cgroups). It **shares the host's kernel**. Light (MBs, milliseconds to start).

### On Windows, there is one extra layer

Containers need a Linux kernel, and Windows does not have one. Docker Desktop therefore runs one small Linux VM using WSL2, and all containers share that VM's kernel:

```
Windows 11
 └─ WSL2 VM (Docker Desktop's Linux kernel)
     ├─ container: make engine   (compiles)
     └─ container: make run-sim  (runs)
```

**Why this matters later:** benchmark numbers measured here include the WSL2 VM, so they will differ from bare-metal Linux. Every benchmark result must say where it ran.

## Every flag in our `docker run` commands

From the [Makefile](../../Makefile):

```bash
docker run --rm -it --init -v "C:/Users/hp/Desktop/vortex:/src" -w /src \
    --name vortex-exchange -p 9000:9000 \
    vortex/engine-build:m0  build/engine-release/src/exchange 9000
```

| Flag                      | Meaning                                                             | What happens without it                         |
| ------------------------- | ------------------------------------------------------------------- | ----------------------------------------------- |
| `--rm`                    | Delete the container when it exits                                  | Stopped containers pile up (`docker ps -a`)     |
| `-i`                      | Keep standard input open                                            | You cannot type into the trader terminal        |
| `-t`                      | Allocate a terminal (TTY)                                           | No line editing; output may buffer oddly        |
| `--init`                  | Run a tiny init process (`tini`) as PID 1, which starts our program | Ctrl+C may be ignored (see below)               |
| `-v host:container`       | **Bind mount**: the Windows folder appears at `/src` inside         | The container sees no source code               |
| `-w /src`                 | Working directory for the command                                   | Relative paths like `build/...` fail            |
| `--name`                  | A fixed name, so `docker exec` can find it                          | Docker picks a random name like `quirky_turing` |
| `-p 9000:9000`            | Forward Windows port 9000 to container port 9000                    | Clients outside the container cannot connect    |
| `vortex/engine-build:m0`  | The image to start from                                             | —                                               |
| `build/.../exchange 9000` | The command to run inside                                           | The image's default (`bash`) runs instead       |

### Bind mounts: why nothing is lost when a container is deleted

```
Windows disk                              inside the container
C:\Users\hp\Desktop\vortex\    <====>     /src/
   engine\src\...                            engine/src/...
   build\engine-release\exchange             build/engine-release/exchange
```

It is the **same folder**, seen from two sides. When `make engine` compiles, the binaries are written to `/src/build/...`, which really means `C:\...\vortex\build\...`. The container is deleted afterwards; the files stay. The next container (`make run-sim`) mounts the same folder and finds them.

Those binaries are **Linux** programs (ELF format). You can see them in Windows Explorer, but you cannot double-click them; they only run inside a Linux container.

### PID 1 and why `--init` is needed

Inside a container, the first process gets process ID 1. On Linux, PID 1 is special: the kernel **does not apply default signal actions** to it. Normally Ctrl+C sends `SIGINT` and the default action kills the process. For PID 1 that default is skipped, so a program that does not install its own handler would just ignore Ctrl+C.

Our programs do install a handler (`std::signal(SIGINT, on_signal)`), so they would mostly work. `--init` makes it robust anyway: `tini` becomes PID 1, forwards signals to our program, and also cleans up "zombie" child processes. Cheap insurance.

### `docker run` vs `docker exec`

- `docker run` creates a **new** container from an image.
- `docker exec` runs an extra command **inside an existing, running** container.

`make run-bots` and `make run-trader` use `docker exec vortex-exchange ...`, so the bots run in the same container as the exchange and reach it at `127.0.0.1:9000`. That is why `run-exchange` must be started first.

## Useful commands

```bash
docker ps                       # running containers
docker ps -a                    # including stopped ones
docker images                   # images on disk
docker logs vortex-exchange     # output of a (detached) container
docker exec -it vortex-exchange bash   # open a shell inside the running exchange container
docker run --rm -it -v "C:/Users/hp/Desktop/vortex:/src" -w /src vortex/engine-build:m0 bash   # a throwaway shell in the build environment
```

The last one is the best way to explore. Inside it, try `gcc --version`, `ls /src`, `uname -a` (shows the WSL2 kernel), and `file build/engine-release/src/exchange` after installing `file` with `apt-get update && apt-get install -y file`.

## Check yourself

1. What is the difference between an image and a container?
2. Why does a container start in milliseconds while a VM takes much longer?
3. You delete every container. Are the compiled binaries gone? Why not?
4. Why pin `ubuntu:24.04` by digest instead of just the tag? What do you give up?
5. What would go wrong without `-p 9000:9000`? Without `--init`?
6. Why can the bots reach the exchange at `127.0.0.1` when started with `make run-bots`?

## Go deeper

- Linux namespaces and cgroups (how isolation actually works)
- Dockerfile best practices: layer ordering, multi-stage builds (we will use one later for small runtime images)
- WSL2 architecture
- Bind mounts vs named volumes (Kafka and MinIO will use volumes in M3/M6)
