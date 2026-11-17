plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
}

// The integration tests run the JVM client against a real Router and the Go
// conformance fixture; both are built from this repository with `go build`.
val repoRoot = rootDir.parentFile
val goBin = layout.buildDirectory.dir("go-bin")

val buildGoBinaries by tasks.registering(Exec::class) {
    description = "Builds meshcall-router and the Go conformance fixture for the integration tests."
    val out = goBin.get().asFile
    inputs.dir(File(repoRoot, "meshcall-router")).withPropertyName("router")
    inputs.dir(File(repoRoot, "meshcall-go")).withPropertyName("goRuntime")
    outputs.dir(out)
    isIgnoreExitValue = false
    commandLine(
        "sh", "-c",
        "command -v go >/dev/null 2>&1 || { echo 'go not found: integration tests will be skipped'; exit 0; }; " +
            "(cd '${repoRoot}/meshcall-router' && go build -o '${out}/meshcall-router' ./cmd/meshcall-router && go build -o '${out}/meshcall' ./cmd/meshcall) && " +
            "(cd '${repoRoot}/meshcall-go' && go build -o '${out}/conformance-fixture' ./conformance/fixture)",
    )
}

extra["goBin"] = goBin
extra["buildGoBinaries"] = buildGoBinaries
