# Aerogel (KO)

[Go to English](README.en.md)

**Aerogel는 아직 완성되지 않았습니다.**  
이 프로젝트는 **개발 단계의 Minecraft 서버 엔진**(Kotlin, 커스텀 프로토콜/런타임 경로)이며, 아직 프로덕션 대체 목적의 완성품이 아닙니다.

이 저장소는 사용 단계보다 엔진 개발과 기여 협업에 초점을 맞춥니다.

- 현재 목표 기준: 바닐라 기능 약 **30% 수준**
- 목표: 정확성, 성능, 구조 명확성
- 상태: 활발한 개발 중, 동작 변화 빈번

---

## 프로젝트 단계 안내 (먼저 읽어주세요)

지금 Aerogel는 다음 목적에 맞습니다:

- 완성품 사용이 아니라 엔진 검증/실험
- 프로토콜/월드 동기화 엣지 케이스 리포트
- 아키텍처 의사결정 초기 단계 기여
- 네트워크/월드/저장/게임플레이 코어 직접 기여

현재는 contributor-first 단계입니다.

## Aerogel가 지향하는 방향

- 기존 코어 패치가 아닌 엔진 레벨 제어
- 고성능 패킷/청크 파이프라인
- 바닐라 호환 월드/플레이어 저장 경로
- 재현 가능하고 검증 가능한 런타임

## 현재 현실 (중요)

- 바닐라 메커닉 전부 구현 전
- 커밋 간 동작 변화 가능
- 리팩터링 시 호환 가정 변경 가능
- 빠른 반복 개발로 인한 브레이킹 체인지 가능

## 기여하고 싶다면

다음 영역의 기여를 적극적으로 환영합니다:

- 바닐라 패리티 구현
- 프로토콜 정확성/클라이언트 동기화 버그 수정
- 저장 포맷 검증/안정화
- 성능 프로파일링/회귀 추적
- 툴체인, DX, 문서, 테스트 하네스

Discord에서 직접 소통해주세요:

**https://discord.gg/fBBgMXXv8P**

## API 패키지 (Maven)

`main` 브랜치 커밋 기준으로 API 모듈이 GitHub Packages에 자동 배포됩니다.

- Group: `org.macaroon3145`
- Artifact: `aerogel-api`
- Version: `1.0-SNAPSHOT`

레포지토리:

```kotlin
repositories {
    maven("https://maven.pkg.github.com/MCAerogel/Aerogel")
}
```

의존성:

```kotlin
dependencies {
    implementation("org.macaroon3145:aerogel-api:1.0-SNAPSHOT")
}
```

## 개발용 실행

요구사항:

- Java 25

빌드:

```bash
./gradlew build
```

실행:

```bash
./gradlew runAerogelJar
```

또는

```bash
java --enable-native-access=ALL-UNNAMED \
     --sun-misc-unsafe-memory-access=allow \
     -Xms4G -Xmx4G \
     -jar build/libs/Aerogel.jar
```

---

## Development Notice

Aerogel는 개발 빌드입니다.  
미구현 기능, 브레이킹 체인지, 빠른 반복 개발을 전제로 합니다.

주의해서 사용하고, 가능하면 기여로 함께 완성해 주세요.
