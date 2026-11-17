rootProject.name = "meshcall-jvm"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    // Downloads the JDK 17 toolchain when only another JDK (say 21) is installed.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include("meshcall", "examples")
