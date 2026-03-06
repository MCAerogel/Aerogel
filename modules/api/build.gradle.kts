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
            artifactId = "api"
            version = project.version.toString()
        }
    }
}
