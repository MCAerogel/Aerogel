plugins {
    kotlin("jvm") version "2.3.0"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("net.fabricmc:mapping-io:0.7.1")
}

kotlin {
    jvmToolchain(25)
}
