import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    application
}

kotlin {
    jvmToolchain(17)
    compilerOptions { jvmTarget.set(JvmTarget.JVM_11) }
}

tasks.withType<JavaCompile>().configureEach { options.release.set(11) }

dependencies {
    implementation(project(":meshcall"))
}

application {
    // ./gradlew :examples:run (Kotlin) or -PmainClass=…JavaShowcase (Java), with MESHCALL_ROUTER_URL set.
    mainClass.set(providers.gradleProperty("mainClass").orElse("io.github.t4wefan.meshcall.examples.ShowcaseDemoKt"))
}
