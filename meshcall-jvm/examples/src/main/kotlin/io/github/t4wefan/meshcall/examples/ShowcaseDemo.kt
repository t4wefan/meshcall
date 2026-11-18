package io.github.t4wefan.meshcall.examples

import io.github.t4wefan.meshcall.CallOptions
import io.github.t4wefan.meshcall.Mesh
import io.github.t4wefan.meshcall.Router
import io.github.t4wefan.meshcall.examples.showcase.NumberItem
import io.github.t4wefan.meshcall.examples.showcase.ShowcaseServiceClient
import io.github.t4wefan.meshcall.examples.showcase.WelcomeRequest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking

/**
 * Calls the showcase demo service (demo/showcase) through the Router in
 * MESHCALL_ROUTER_URL with the generated Kotlin client.
 */
fun main() = runBlocking {
    Mesh(Router()).use { mesh ->
        val client = ShowcaseServiceClient(mesh).withOptions(
            CallOptions.DEFAULT.withTimeoutMs(10_000).withMetadataEntry("x-caller-id", "kotlin-showcase"),
        )
        println("1. query with expanded parameters")
        println("   " + client.greet("MeshCall", repeat = 2).message)
        println("2. query with a request object")
        val welcome = client.welcome(WelcomeRequest(name = "Kotlin", tags = listOf("typed", "coroutines")))
        println("   ${welcome.message} tags=${welcome.tags}")
        println("3. mutating call with metadata injection")
        val visit = client.visit("MeshCall")
        println("   visits=${visit.visits} caller=${visit.caller}")
        println("4. server stream")
        println("   received=" + client.count(3, 7).toList().map { it.value })
        println("5. client stream")
        println("   total=" + client.sumValues(10, flowOf(NumberItem(2), NumberItem(3), NumberItem(5))).total)
    }
}
