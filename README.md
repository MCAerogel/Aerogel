# Aerogel

[한국어 문서로 이동](README.ko.md)

**Aerogel is not finished.**  
This is a **development-stage Minecraft server engine** (Kotlin, custom protocol/runtime path), not a production-ready Paper replacement yet.

This repository is focused on engine development and contributor collaboration.

- Progress target today: roughly **~30% of vanilla feature coverage**
- Goal: correctness, performance, and architecture clarity
- Status: active development, frequent behavioral changes

---

## Project Stage (Read First)

Use Aerogel now if you want to:

- test an evolving engine, not a completed product
- report edge cases and protocol/world-sync bugs early
- shape architecture decisions before they are frozen
- contribute code to core systems (network, world, storage, gameplay)

This project is in a contributor-first stage.

## What Aerogel is building toward

- independent engine-level control (not just patching an existing server core)
- high-performance packet and chunk pipeline
- vanilla-compatible world/player persistence path
- reproducible, testable runtime behavior for future scale

## Current reality (important)

- not all vanilla mechanics are implemented
- behavior can change between commits
- compatibility assumptions can break during refactors
- expect rapid iteration and breaking changes between commits

## If you want to help

We are actively looking for contributors who can help with:

- vanilla parity implementation
- protocol correctness and client sync edge cases
- persistence/format validation
- performance profiling and regression tracking
- toolchain, DX, docs, test harnesses

Join Discord and talk directly with the team:

**https://discord.gg/fBBgMXXv8P**

## API Package (Maven)

API module is published automatically to GitHub Packages on commits to `main`.

- Group: `org.macaroon3145`
- Artifact: `aerogel-api`
- Version: `1.0-SNAPSHOT`

Repository:

```kotlin
repositories {
    maven("https://maven.pkg.github.com/MCAerogel/Aerogel")
}
```

Dependency:

```kotlin
dependencies {
    compileOnly("org.macaroon3145:aerogel-api:1.0-SNAPSHOT")
}
```

## Quick Start (Dev)

Requirements:

- Java 25

Build:

```bash
./gradlew build
```

Run:

```bash
./gradlew runAerogelJar
```

or

```bash
java --enable-native-access=ALL-UNNAMED \
     --sun-misc-unsafe-memory-access=allow \
     -Xms4G -Xmx4G \
     -jar build/libs/Aerogel.jar
```

---

## Development Notice

Aerogel is a development build.  
Expect incomplete features, breaking changes, and rapid iteration.

Use with caution. Contribute if you want to accelerate it.
