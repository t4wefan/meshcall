import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    `java-library`
}

group = "io.github.t4wefan"
version = "0.4.0"

kotlin {
    jvmToolchain(17)
    explicitApi()
    compilerOptions {
        // Java 11 bytecode: every current Android toolchain (D8/R8) accepts it.
        jvmTarget.set(JvmTarget.JVM_11)
        // Metadata an app compiled with Kotlin 2.1 or later can read (a compiler
        // reads one version ahead), whatever compiler builds the runtime.
        apiVersion.set(org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_2_2)
        languageVersion.set(org.jetbrains.kotlin.gradle.dsl.KotlinVersion.KOTLIN_2_2)
        jvmDefault.set(org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode.ENABLE)
    }
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
}

tasks.withType<JavaCompile>().configureEach { options.release.set(11) }

// Every Kotlin golden of the code generator compiles against this runtime
// (router/ and quickstart/ pin the same contract, so one of them suffices).
sourceSets["test"].kotlin {
    srcDir(rootDir.parentFile.resolve("meshcall-router/internal/codegen/testdata"))
    exclude("router/**")
}

dependencies {
    api(libs.coroutines.core)
    api(libs.serialization.json)
    api(libs.okhttp)
    implementation(libs.bouncycastle)

    testImplementation(kotlin("test"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.coroutines.test)
    testRuntimeOnly(libs.junit.launcher)
}

val goBin = rootProject.extra["goBin"] as Provider<*>

tasks.test {
    useJUnitPlatform()
    dependsOn(rootProject.extra["buildGoBinaries"] as TaskProvider<*>)
    val dir = (goBin.get() as Directory).asFile
    environment("MESHCALL_TEST_BIN", dir.absolutePath)
    systemProperty("meshcall.repoRoot", rootDir.parentFile.absolutePath)
    testLogging {
        events("failed", "skipped")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
