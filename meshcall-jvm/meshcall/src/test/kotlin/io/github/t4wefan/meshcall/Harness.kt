package io.github.t4wefan.meshcall

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Assumptions.assumeTrue
import java.io.File
import java.security.SecureRandom
import java.util.concurrent.TimeUnit

const val FIXTURE = "conformance.v1.Fixture"
const val TARGETED = "conformance.v1.Targeted"
const val VAULT = "conformance.v1.Vault"
const val INSTANCE = "fixture-1"

/**
 * A real Router (development mode: no accounts) and the Go conformance
 * fixture (meshcall-router/conformance/FIXTURE.md), built by the
 * `buildGoBinaries` Gradle task. Tests are skipped when Go is unavailable.
 */
class Env private constructor(val router: RouterProcess, val instance: String) : AutoCloseable {
    @Volatile var fixture: Process = startFixture()
        private set

    val url: String get() = router.url

    fun mesh(options: MeshOptions = MeshOptions.DEFAULT): Mesh = Mesh(Router(url), options)

    fun startFixture(): Process {
        val process = ProcessBuilder(File(bin(), "conformance-fixture").path)
            .redirectErrorStream(true)
            .redirectOutput(File.createTempFile("fixture", ".log"))
            .also {
                it.environment()["MESHCALL_ROUTER_URL"] = url
                it.environment()["MESHCALL_CONFORMANCE_INSTANCE"] = instance
                it.environment()["MESHCALL_PING_INTERVAL_MS"] = "1000"
                it.environment()["MESHCALL_E2E_PRIVATE_KEY"] = E2E_PRIVATE
            }
            .start()
        fixture = process
        return process
    }

    /** Waits until the fixture's services are in the Router's Registry. */
    fun awaitRegistered(timeoutMs: Long = 20_000) = runBlocking {
        Mesh(Router(url)).use { mesh ->
            withTimeout(timeoutMs) {
                while (true) {
                    val ready = try {
                        mesh.registry.list().any { it.name == FIXTURE && it.instances.any { i -> i.id == instance } }
                    } catch (_: MeshCallException) {
                        false
                    }
                    if (ready) return@withTimeout
                    delay(50)
                }
            }
        }
    }

    override fun close() {
        fixture.destroyForcibly()
        router.close()
    }

    companion object {
        private val random = SecureRandom()
        val E2E_PRIVATE: String = b64encode(ByteArray(32).also { random.nextBytes(it) })
        val E2E_PUBLIC: String = b64encode(x25519Public(b64decode(E2E_PRIVATE)))

        fun bin(): File {
            val dir = System.getenv("MESHCALL_TEST_BIN")?.let(::File)
            assumeTrue(
                dir != null && File(dir, "meshcall-router").canExecute() && File(dir, "conformance-fixture").canExecute(),
                "meshcall-router / conformance-fixture not built (needs Go): integration tests skipped",
            )
            return dir!!
        }

        fun start(instance: String = INSTANCE, routerArgs: List<String> = emptyList()): Env {
            val env = Env(RouterProcess.start(routerArgs), instance)
            env.awaitRegistered()
            return env
        }

        /** One Router and fixture for the read-mostly tests. */
        val shared: Env by lazy {
            start().also { env -> Runtime.getRuntime().addShutdownHook(Thread { env.close() }) }
        }
    }
}

class RouterProcess private constructor(private val process: Process, val port: Int) : AutoCloseable {
    val url: String get() = "ws://127.0.0.1:$port"

    override fun close() {
        process.destroy()
        if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly().waitFor()
    }

    companion object {
        fun start(args: List<String> = emptyList(), port: Int = 0): RouterProcess {
            val process = ProcessBuilder(listOf(File(Env.bin(), "meshcall-router").path, "--port", port.toString()) + args)
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .start()
            // The first stdout line announces where it listens.
            val reader = process.inputStream.bufferedReader()
            val line = reader.readLine() ?: error("meshcall-router exited")
            Thread { reader.forEachLine { } }.apply { isDaemon = true }.start()
            val ready = Json.parseToJsonElement(line).jsonObject
            check(ready["kind"]?.jsonPrimitive?.content == "router.ready") { "unexpected first line: $line" }
            return RouterProcess(process, ready["port"]!!.jsonPrimitive.int)
        }
    }
}
