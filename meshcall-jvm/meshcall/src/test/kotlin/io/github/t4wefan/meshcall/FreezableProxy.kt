package io.github.t4wefan.meshcall

import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList

/**
 * A TCP relay that can stop passing bytes without closing anything: a peer
 * that vanished without a FIN, as a phone that lost its network or an app
 * frozen in the background looks to the other end.
 */
class FreezableProxy(private val targetPort: Int) : AutoCloseable {
    private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
    private val sockets = CopyOnWriteArrayList<Socket>()
    @Volatile var frozen = false

    val url: String get() = "ws://127.0.0.1:${server.localPort}"

    init {
        Thread {
            while (!server.isClosed) {
                val client = try { server.accept() } catch (_: Exception) { break }
                val target = Socket(InetAddress.getLoopbackAddress(), targetPort)
                sockets += client
                sockets += target
                pump(client.getInputStream(), target.getOutputStream())
                pump(target.getInputStream(), client.getOutputStream())
            }
        }.apply { isDaemon = true }.start()
    }

    private fun pump(input: InputStream, output: OutputStream) {
        Thread {
            val buffer = ByteArray(16 * 1024)
            try {
                while (true) {
                    while (frozen) Thread.sleep(5)
                    val n = input.read(buffer)
                    if (n < 0) break
                    while (frozen) Thread.sleep(5)
                    output.write(buffer, 0, n)
                    output.flush()
                }
            } catch (_: Exception) {
            }
        }.apply { isDaemon = true }.start()
    }

    /** Drops every relayed connection (abruptly) and passes bytes again. */
    fun reset() {
        for (socket in sockets) try { socket.close() } catch (_: Exception) {}
        sockets.clear()
        frozen = false
    }

    override fun close() {
        server.close()
        reset()
    }
}
