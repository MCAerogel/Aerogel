pluginManagement {
    repositories {
        gradlePluginPortal()
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "Aerogel"
include(":api")
project(":api").projectDir = file("modules/api")
include(":blockEditor")
project(":blockEditor").projectDir = file("modules/blockEditor")
include(":aerogelWorldGen")
project(":aerogelWorldGen").projectDir = file("../Aerogel-WorldGen")
