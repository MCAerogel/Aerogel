plugins {
    kotlin("jvm") version "2.3.0"
    `maven-publish`
}

group = "org.macaroon3145"
version = "1.0-SNAPSHOT"

kotlin {
    jvmToolchain(25)
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.10.0")
}

tasks.named<Jar>("jar") {
    archiveBaseName.set("AerogelAPI")
    archiveVersion.set("")
    archiveClassifier.set("")
    destinationDirectory.set(rootProject.layout.buildDirectory.dir("libs"))
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            groupId = project.group.toString()
            artifactId = "aerogel-api"
            version = project.version.toString()
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/MCAerogel/Aerogel")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                    ?: findProperty("gpr.user")?.toString()
                password = System.getenv("GITHUB_TOKEN")
                    ?: findProperty("gpr.key")?.toString()
            }
        }
    }
}
