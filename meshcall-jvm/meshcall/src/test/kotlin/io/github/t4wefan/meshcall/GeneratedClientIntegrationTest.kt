package io.github.t4wefan.meshcall

import io.github.t4wefan.meshcall.conformance.FixtureClient
import io.github.t4wefan.meshcall.conformance.TargetedClient
import io.github.t4wefan.meshcall.conformance.Text
import io.github.t4wefan.meshcall.conformance.Value
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** The generated Kotlin clients (scripts/regenerate.sh kotlin) against the fixture. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GeneratedClientIntegrationTest {
    private lateinit var mesh: Mesh
    private lateinit var fixture: FixtureClient

    @BeforeAll
    fun setUp() {
        mesh = Env.shared.mesh()
        fixture = FixtureClient(mesh)
    }

    @AfterAll
    fun tearDown() {
        if (::mesh.isInitialized) mesh.close()
    }

    private fun test(block: suspend CoroutineScope.() -> Unit) = runBlocking { withTimeout(30_000) { block() } }

    @Test
    fun everyShapeIsTyped() = test {
        assertEquals("hi", fixture.echo(5, "hi").text)
        assertEquals((0L until 20L).toList(), fixture.count(20).toList().map { it.index })
        assertEquals(106L, fixture.sum(offset = 100, items = (1L..3L).map { Value(it) }.asFlow()).total)
        fixture.chat(prefix = "k:").use { call ->
            val out = async { call.output.toList().map { it.text } }
            call.send(Text("one"))
            call.closeSend()
            assertEquals(listOf("k:one", "done:1"), out.await())
        }
    }

    @Test
    fun targetedClientsAreBoundToTheInstance() = test {
        assertEquals(INSTANCE, TargetedClient(mesh, INSTANCE).whoami().instance)
    }

    // conformance-scenario: unary.undecodable_response
    @Test
    fun aResponseThatBreaksTheClientsContractIsAProtocolError() = test {
        // echo answers {value, text}; a client whose type wants `tag` cannot decode it.
        val error = assertFailsWith<ProtocolErrorException> {
            mesh.unary(
                FIXTURE, "echo", io.github.t4wefan.meshcall.conformance.Echo(1, "x"),
                io.github.t4wefan.meshcall.conformance.Echo.serializer(), io.github.t4wefan.meshcall.conformance.Tag.serializer(),
            )
        }
        assertEquals("client", error.source)
        assertEquals("invalid_response", error.reason)
        assertTrue(error.violations.isNotEmpty())
    }

    @Test
    fun withOptionsAppliesToEveryCall() = test {
        val error = assertFailsWith<DeadlineExceededException> {
            fixture.withOptions(CallOptions.DEFAULT.withTimeoutMs(200)).slow(5_000)
        }
        assertEquals(false, error.retryable)
    }
}
