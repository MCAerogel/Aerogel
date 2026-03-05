<div align="center">

# Aerogel (KO)

**Kotlin 기반 개발 단계 Minecraft 서버 엔진**

[![상태](https://img.shields.io/badge/%EC%83%81%ED%83%9C-%ED%99%9C%EB%B0%9C%ED%95%9C%20%EA%B0%9C%EB%B0%9C%20%EC%A4%91-ff9800?style=for-the-badge)](#%EA%B0%9C%EB%B0%9C-%EC%95%88%EB%82%B4)
[![Java](https://img.shields.io/badge/Java-25-007396?style=for-the-badge&logo=openjdk&logoColor=white)](#%EB%B9%A0%EB%A5%B8-%EC%8B%9C%EC%9E%91)
[![Kotlin](https://img.shields.io/badge/Kotlin-2.3.0-7f52ff?style=for-the-badge&logo=kotlin&logoColor=white)](#%ED%98%84%EC%9E%AC-%EA%B5%AC%ED%98%84-%EA%B8%B0%EB%B0%98)
[![API](https://img.shields.io/badge/API-aerogel--api-2ea44f?style=for-the-badge)](#api-%EB%B0%B0%ED%8F%AC)

[![README EN](https://img.shields.io/badge/README-English-111111?style=for-the-badge)](README.md)
[![Discord](https://img.shields.io/badge/Discord-%EC%BB%A4%EB%AE%A4%EB%8B%88%ED%8B%B0%20%EC%B0%B8%EC%97%AC-5865f2?style=for-the-badge&logo=discord&logoColor=white)](https://discord.gg/fBBgMXXv8P)

</div>

---

| 빠른 이동 | |
|---|---|
| 시작하기 | [빠른 시작](#빠른-시작) |
| 플러그인 개발 | [플러그인 개발](#플러그인-개발) |
| API 패키지 | [API 배포](#api-배포) |
| 기여 | [기여 우선 영역](#기여-우선-영역) |

## 프로젝트 스냅샷

> Aerogel는 아직 프로덕션용 엔진이 아닙니다. 커밋 간 동작 변경(브레이킹 체인지 포함)이 발생할 수 있습니다.

- 목표: 정확성, 성능, 플러그인/런타임 아키텍처
- 스택: Kotlin + Netty + 커스텀 프로토콜/런타임
- Java 요구 버전: 25
- 바닐라 패리티: 부분 구현, 지속 확장 중

## 현재 구현 기반

| 영역 | 현재 상태 |
|---|---|
| 네트워킹 | Netty 파이프라인 + 프로토콜 처리 경로 |
| 월드/저장 | `level.dat` 시드/시간-날씨 메타데이터 경로 |
| 런타임 | 게임 루프 + 성능 모니터링 기초 |
| 플러그인 | 런타임, 이벤트 버스, 커맨드 연동 |
| 핫리로드 | `plugins/*.jar` 변경 감지 루프 |
| i18n | 서버 리소스 `en`, `ko` 제공 |
| 브리지 | 메인 jar에 Folia 브리지 아티팩트 내장 |

## 빌드 산출물

- `build/libs/Aerogel.jar` - 메인 서버 런타임
- `build/libs/AerogelFoliaBridge.jar` - Folia 브리지 플러그인
- `:api` 모듈 - `org.macaroon3145:aerogel-api:1.0-SNAPSHOT`

## 빠른 시작

```bash
./gradlew build
./gradlew runAerogelJar
```

수동 실행:

```bash
java --enable-native-access=ALL-UNNAMED \
     --sun-misc-unsafe-memory-access=allow \
     -Xms4G -Xmx4G \
     -jar build/libs/Aerogel.jar
```

런타임 경로:

- `aerogel.properties`
- `plugins/`
- `world/`
- `logs/`

## 플러그인 개발

의존성 설정:

```kotlin
repositories {
    mavenCentral()
    maven("https://maven.pkg.github.com/MCAerogel/Aerogel")
}

dependencies {
    compileOnly("org.macaroon3145:aerogel-api:1.0-SNAPSHOT")
}
```

최소 엔트리 예시:

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

## API 배포

`main` 브랜치에서 `modules/api/**` 변경 시 GitHub Actions를 통해 GitHub Packages로 자동 배포됩니다.

- Group: `org.macaroon3145`
- Artifact: `aerogel-api`
- Version: `1.0-SNAPSHOT`
- Repository: `https://maven.pkg.github.com/MCAerogel/Aerogel`

## 기여 우선 영역

- 바닐라 패리티 및 동작 검증
- 프로토콜 정확성/동기화 엣지 케이스
- 저장 포맷 검증 및 마이그레이션 안정화
- 성능 프로파일링 및 회귀 추적
- 테스트, 툴링, 개발자 경험(DX)

커뮤니티: `https://discord.gg/fBBgMXXv8P`

## 개발 안내

핵심 아키텍처 안정화 전까지 빠른 반복 개발이 계속됩니다.

