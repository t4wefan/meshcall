package io.github.t4wefan.meshcall.examples;

import io.github.t4wefan.meshcall.CallOptions;
import io.github.t4wefan.meshcall.Mesh;
import io.github.t4wefan.meshcall.Router;
import io.github.t4wefan.meshcall.StreamObserver;
import io.github.t4wefan.meshcall.examples.showcase.CountItem;
import io.github.t4wefan.meshcall.examples.showcase.NumberItem;
import io.github.t4wefan.meshcall.examples.showcase.ShowcaseServiceClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** The showcase from Java: futures for calls, an observer for the stream. */
public final class JavaShowcase {
    public static void main(String[] args) throws Exception {
        try (Mesh mesh = new Mesh(new Router())) {
            ShowcaseServiceClient client = new ShowcaseServiceClient(mesh)
                .withOptions(CallOptions.DEFAULT.withTimeoutMs(10_000L));
            System.out.println("1. " + client.greetAsync("MeshCall", 2L).get().getMessage());

            CompletableFuture<Void> done = new CompletableFuture<>();
            client.countAsync(3, 7, new StreamObserver<CountItem>() {
                @Override public void onNext(CountItem item) { System.out.println("   item " + item.getValue()); }
                @Override public void onCompleted(Map<String, String> trailers) { done.complete(null); }
                @Override public void onError(Throwable error) { done.completeExceptionally(error); }
            });
            done.get();

            long total = client.sumValuesAsync(10, List.of(new NumberItem(2), new NumberItem(3), new NumberItem(5))).get().getTotal();
            System.out.println("3. total=" + total);
        }
    }
}
