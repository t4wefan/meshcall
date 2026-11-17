package io.github.t4wefan.meshcall

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Assumptions.assumeTrue
import java.io.BufferedReader
import java.io.File
import java.io.Writer
import java.util.concurrent.TimeUnit

const val FIXTURE = "conformance.v1.Fixture"
const val TARGETED = "conformance.v1.Targeted"
const val VAULT = "conformance.v1.Vault"
const val INSTANCE = "fixture-1"

/**
 * The shared client conformance environment (meshcall-router/conformance/
 * CLIENTS.md): `meshcall-conformance env` runs a Router behind a pausing
 * proxy and a fixture service, and restarts or stalls them on command. The
 * fixture is the Go one unless `MESHCALL_FIXTURE` names another runtime's.
 * Both binaries come from the `buildGoBinaries` Gradle task (or
 * `MESHCALL_CONFORMANCE_BINARY` / `MESHCALL_FIXTURE`); without them the
 * integration tests are skipped.
 */
class Env private constructor(
    private val process: Process,
    private val commands: Writer,
    private val answers: BufferedReader,
    val url: String,
    val instance: String,
    /** The fixture's end-to-end public key. */
    val e2eKey: String,
) : AutoCloseable {

    fun mesh(options: MeshOptions = MeshOptions.DEFAULT): Mesh = Mesh(Router(url), options)

    /** Sends one command (`restart_router`, `pause_router`, …) and waits until it is done. */
    @Synchronized
    fun command(name: String) {
        commands.write("{\"command\":\"$name\"}\n")
        commands.flush()
        val answer = answers.readLine() ?: error("meshcall-conformance env exited")
        val reply = Json.parseToJsonElement(answer).jsonObject
        check(reply["ok"]?.jsonPrimitive?.content == "true") { "$name failed: $answer" }
    }

    override fun close() {
        try {
            commands.close() // the environment stops when its stdin ends
        } catch (_: Exception) {
        }
        if (!process.waitFor(10, TimeUnit.SECONDS)) process.destroyForcibly()
    }

    companion object {
        private fun bin(name: String): File? =
            System.getenv("MESHCALL_TEST_BIN")?.let { File(it, name) }?.takeIf { it.canExecute() }

        fun start(): Env {
            val conformance = System.getenv("MESHCALL_CONFORMANCE_BINARY")?.takeIf { it.isNotBlank() }?.let(::File)
                ?: bin("meshcall-conformance")
            val fixture = System.getenv("MESHCALL_FIXTURE")?.takeIf { it.isNotBlank() } ?: bin("conformance-fixture")?.path
            assumeTrue(
                conformance != null && conformance.canExecute() && fixture != null,
                "meshcall-conformance / conformance-fixture not built (needs Go): integration tests skipped",
            )
            val root = File(System.getProperty("meshcall.repoRoot") ?: "../..")
            val process = ProcessBuilder(conformance!!.path, "env", "--fixture", fixture!!)
                .directory(root)
                .redirectError(ProcessBuilder.Redirect.appendTo(File.createTempFile("conformance-env", ".log")))
                .start()
            val answers = process.inputStream.bufferedReader()
            val line = answers.readLine() ?: error("meshcall-conformance env did not start")
            val ready = Json.parseToJsonElement(line).jsonObject
            check(ready["kind"]?.jsonPrimitive?.content == "env.ready") { "unexpected first line: $line" }
            fun field(name: String) = ready[name]!!.jsonPrimitive.content
            return Env(
                process, process.outputStream.bufferedWriter(), answers,
                field("router_url"), field("instance_id"), field("e2e_key"),
            )
        }

        /** One environment for the whole test run; disruptive tests restore it before returning. */
        val shared: Env by lazy {
            start().also { env -> Runtime.getRuntime().addShutdownHook(Thread { env.close() }) }
        }
    }
}
