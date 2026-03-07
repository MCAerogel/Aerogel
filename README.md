<div align="center">

<img src="https://avatars.githubusercontent.com/u/264545382?s=200&v=4" width="120" height="120" alt="Aerogel logo" style="border-radius: 50%; margin: 30px auto 4px auto; display: block;"/>

<h1 style="margin: 0 0 10px 0;">Aerogel</h1>

**Development-stage Minecraft server engine in Kotlin**

[![Stage](https://img.shields.io/badge/Stage-Active%20Development-ff9800?style=for-the-badge)](#development-notice)
[![Java](https://img.shields.io/badge/Java-25-007396?style=for-the-badge&logo=openjdk&logoColor=white)](#quick-start)
[![Kotlin](https://img.shields.io/badge/Kotlin-2.3.0-7f52ff?style=for-the-badge&logo=kotlin&logoColor=white)](#implemented-foundation)
[![API](https://img.shields.io/badge/API-aerogel--api-2ea44f?style=for-the-badge)](#api-publishing)

[![README KO](https://img.shields.io/badge/README-%ED%95%9C%EA%B5%AD%EC%96%B4-0a66c2?style=for-the-badge)](README.ko.md)
[![Downloads](https://img.shields.io/badge/Downloads-Release%20Site-1f883d?style=for-the-badge)](https://mcaerogel.github.io/Aerogel-Web/)
[![Discord](https://img.shields.io/badge/Discord-Join%20Community-5865f2?style=for-the-badge&logo=discord&logoColor=white)](https://discord.gg/fBBgMXXv8P)

</div>

---

<div align="center">

<table>
  <tr>
    <th>Quick Links</th>
    <th></th>
  </tr>
  <tr>
    <td align="center">Start here</td>
    <td align="center"><a href="#quick-start">Quick Start</a></td>
  </tr>
  <tr>
    <td align="center">Plugin dev</td>
    <td align="center"><a href="#plugin-development">Plugin Development</a></td>
  </tr>
  <tr>
    <td align="center">API package</td>
    <td align="center"><a href="#api-publishing">API Publishing</a></td>
  </tr>
  <tr>
    <td align="center">Contribute</td>
    <td align="center"><a href="#contribution-focus">Contribution Focus</a></td>
  </tr>
  <tr>
    <td align="center">Downloads</td>
    <td align="center"><a href="https://mcaerogel.github.io/Aerogel-Web/">Release Site</a></td>
  </tr>
</table>

</div>

## Project Snapshot

> Aerogel is not production-ready yet. It is iteration-heavy and may introduce breaking behavior changes between commits.

- Focus: correctness, performance, plugin/runtime architecture
- Stack: Kotlin + Netty + custom protocol/runtime path
- Java requirement: 25
- Vanilla parity: partial and expanding continuously

## Team

<div align="center">

<table>
  <tr>
    <th>Role</th>
    <th>Profile</th>
  </tr>
  <tr>
    <td align="center">Project Owner</td>
    <td align="center"><img src="https://github.com/Mkkas3145.png" width="64" height="64" alt="Mkkas3145"/><br/><strong>Mkkas3145</strong><br/><a href="https://github.com/Mkkas3145">GitHub</a></td>
  </tr>
  <tr>
    <td align="center">Core Developer</td>
    <td align="center"><img src="https://github.com/Mkkas3145.png" width="64" height="64" alt="Mkkas3145"/><br/><strong>Mkkas3145</strong><br/><a href="https://github.com/Mkkas3145">GitHub</a></td>
  </tr>
  <tr>
    <td align="center">Maintainer</td>
    <td align="center"><img src="https://github.com/uniformization.png" width="64" height="64" alt="uniformization"/><br/><strong>uniformization</strong><br/><a href="https://github.com/uniformization">GitHub</a></td>
  </tr>
</table>

</div>

## Implemented Foundation

| Area | Current State |
|---|---|
| Networking | Netty pipeline + protocol handling path |
| World/Persistence | `level.dat` seed/time-weather metadata path |
| Runtime | game loop + performance monitoring primitives |
| Plugins | runtime, event bus, command integration |
| Hot Reload | `plugins/*.jar` change-detection loop |
| i18n | server resources for `en` and `ko` |
| Bridge | Folia bridge artifact embedded in main jar |

## Build Artifacts

- `build/libs/Aerogel.jar` - main server runtime
- `build/libs/AerogelFoliaBridge.jar` - Folia bridge plugin
- `:api` module - `com.github.MCAerogel:Aerogel:main-SNAPSHOT`

## Quick Start

```bash
./gradlew build
./gradlew runAerogelJar
```

Manual run:

```bash
java --enable-native-access=ALL-UNNAMED \
     --sun-misc-unsafe-memory-access=allow \
     -Xms4G -Xmx4G \
     -jar build/libs/Aerogel.jar
```

Runtime paths:

- `aerogel.properties`
- `plugins/`
- `world/`
- `logs/`

## Plugin Development

Dependency setup:

```kotlin
repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    compileOnly("com.github.MCAerogel:Aerogel:main-SNAPSHOT")
}
```

Minimal plugin entry:

```kotlin
package com.example

import org.macaroon3145.api.plugin.AerogelPlugin
import org.macaroon3145.api.plugin.PluginContext

class HelloPlugin : AerogelPlugin {
    override fun onEnable(context: PluginContext) {
        context.logger.info("Hello from HelloPlugin")
    }
}
```

`src/main/resources/aerogel-plugin.json`:

```json
{
  "id": "hello-plugin",
  "name": "Hello Plugin",
  "version": "0.1.0",
  "apiVersion": "1.0",
  "mainClass": "com.example.HelloPlugin",
  "dependencies": [],
  "softDependencies": []
}
```

## API Publishing

API is built and served by JitPack. GitHub Actions triggers JitPack builds automatically for API changes on `main` (snapshot: `main-SNAPSHOT`) and for tags (release).

- Group: `com.github.MCAerogel`
- Artifact: `Aerogel`
- Version: `main-SNAPSHOT` (or Git tag, e.g. `v1.0.0`)
- Repository: `https://jitpack.io`

## Contribution Focus

- vanilla parity and behavior validation
- protocol correctness and sync edge cases
- persistence/format validation and migrations
- profiling and performance regressions
- tests, tooling, and developer experience

Community: `https://discord.gg/fBBgMXXv8P`

## Development Notice

Aerogel is intentionally iteration-heavy while core architecture stabilizes.
