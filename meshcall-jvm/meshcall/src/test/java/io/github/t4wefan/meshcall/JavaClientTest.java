package io.github.t4wefan.meshcall;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.t4wefan.meshcall.conformance.FixtureClient;
import io.github.t4wefan.meshcall.conformance.Index;
import io.github.t4wefan.meshcall.conformance.Text;
import io.github.t4wefan.meshcall.conformance.Value;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The generated client and the runtime as a Java caller uses them. */
class JavaClientTest {
    private static <T> StreamObserver<T> collect(List<T> into, CompletableFuture<Map<String, String>> done) {
        return new StreamObserver<T>() {
            @Override public void onNext(T item) { into.add(item); }
            @Override public void onCompleted(Map<String, String> trailers) { done.complete(trailers); }
            @Override public void onError(Throwable error) { done.completeExceptionally(error); }
        };
    }

    @Test
    void futuresObserversAndDuplex() throws Exception {
        Env env = Env.Companion.getShared();
        try (Mesh mesh = new Mesh(new Router(env.getUrl()), MeshOptions.DEFAULT.withPingIntervalMs(5_000L))) {
            FixtureClient client = new FixtureClient(mesh);
            assertEquals("java", client.echoAsync(1, "java").get(10, TimeUnit.SECONDS).getText());

            List<Index> items = Collections.synchronizedList(new ArrayList<>());
            CompletableFuture<Map<String, String>> done = new CompletableFuture<>();
            client.countAsync(25, collect(items, done));
            done.get(10, TimeUnit.SECONDS);
            assertEquals(25, items.size());

            long total = client.sumAsync(1L, List.of(new Value(2), new Value(3))).get(10, TimeUnit.SECONDS).getTotal();
            assertEquals(6, total);

            List<Text> output = Collections.synchronizedList(new ArrayList<>());
            CompletableFuture<Map<String, String>> ended = new CompletableFuture<>();
            JavaDuplexCall<Text, Text> chat = client.chatAsync("j:", collect(output, ended)).get(10, TimeUnit.SECONDS);
            chat.send(new Text("a")).get(10, TimeUnit.SECONDS);
            chat.closeSend().get(10, TimeUnit.SECONDS);
            ended.get(10, TimeUnit.SECONDS);
            assertEquals(List.of("j:a", "done:1"), output.stream().map(Text::getText).collect(java.util.stream.Collectors.toList()));

            ExecutionException failed = assertThrows(ExecutionException.class,
                () -> client.failAsync("app.java", "from java").get(10, TimeUnit.SECONDS));
            MeshCallException error = assertInstanceOf(MeshCallException.class, failed.getCause());
            assertEquals("app.java", error.getCode());

            ExecutionException late = assertThrows(ExecutionException.class,
                () -> client.slowAsync(5_000, null, CallOptions.DEFAULT.withTimeoutMs(200L)).get(10, TimeUnit.SECONDS));
            assertInstanceOf(DeadlineExceededException.class, late.getCause());
        }
    }
}
