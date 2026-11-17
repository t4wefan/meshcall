import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    `java-library`
}

group = "io.github.t4wefan"
version = "0.3.0"

kotlin {
    jvmToolchain(17)
    explicitApi()
    compilerOptions {
        // Java 11 bytecode: every current Android toolchain (D8/R8) accepts it.
        jvmTarget.set(JvmTarget.JVM_11)
        jvmDefault.set(org.jetbrains.kotlin.gradle.dsl.JvmDefaultMode.ENABLE)
    }
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
}

tasks.withType<JavaCompile>().configureEach { options.release.set(11) }

// Every Kotlin golden of the code generator compiles against this runtime.
sourceSets["test"].kotlin.srcDir(rootDir.parentFile.resolve("meshcall-router/internal/codegen/testdata"))

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
