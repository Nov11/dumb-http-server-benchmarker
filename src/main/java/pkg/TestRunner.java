package pkg;

import javafx.util.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 输入url，请求总次数，并发度数
 * 输出平均 p95
 * 10 并发 100次
 */
public class TestRunner {
    private static final Logger logger = LoggerFactory.getLogger(TestRunner.class);
    private CloseableHttpAsyncClient httpclient;
    private final String url;
    private final int totalRequestNumber;
    private final int concurrency;
    private final HttpGet getRequest;
    private final ConcurrentLinkedQueue<Long> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pending;

    public TestRunner(String url, int totalRequestNumber, int concurrency) {
        httpclient = HttpAsyncClients.custom().setMaxConnPerRoute(200).setMaxConnTotal(200).build();
        httpclient.start();
        this.url = url;
        this.totalRequestNumber = totalRequestNumber;
        this.concurrency = concurrency;
        this.getRequest = new HttpGet(url);
        this.pending = new AtomicInteger(totalRequestNumber);
    }

    public Pair<Double, Long> run() {
        List<CompletableFuture<Void>> list = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            list.add(send());
        }

        try {
            CompletableFuture.allOf(list.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("error :", e);
        }

        logger.info("completed requests : {}", queue.size());
        List<Long> time = new ArrayList<>(queue);
        time.sort(Long::compare);
        double avg = time.stream().reduce(Long::sum).get() * 1.0 / time.size();
        long p95 = time.get((int) (time.size() * 0.95));
        return new Pair<>(avg, p95);
    }

    public CompletableFuture<Void> send() {
        int seq = pending.getAndDecrement();
        if (seq > 0) {
            CompletableFuture<Void> future = doGet(seq).exceptionally(e -> {
                logger.error("resp error", e);
                return null;
            });
            return future.thenCompose(v -> send());
        }
        return CompletableFuture.completedFuture(null);
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            logger.error("args length < 3");
            return;
        }
        String url = args[0];
        int totalRequestNumber = Integer.parseInt(args[1]);
        int concurrency = Integer.parseInt(args[2]);

        TestRunner testRunner = new TestRunner(url, totalRequestNumber, concurrency);
        Pair<Double, Long> result = testRunner.run();
        logger.info("avg : {}, p95 : {}", result.getKey(), result.getValue());
        System.exit(0);
    }

    private CompletableFuture<Void> doGet(int seq) {
        CallBack result = new CallBack(queue, seq);
        httpclient.execute(getRequest, result);
        return result;
    }

    private static class CallBack extends CompletableFuture<Void> implements FutureCallback<HttpResponse> {
        private final long start = System.currentTimeMillis();
        private final Queue<Long> queue;
        private final int sequence;

        public CallBack(Queue<Long> queue, int seq) {
            this.queue = queue;
            this.sequence = seq;
        }

        public void completed(HttpResponse result) {
            save();
            this.complete(null);
        }

        public void failed(Exception ex) {
            save();
            this.completeExceptionally(ex);
        }

        public void cancelled() {
            save();
            this.completeExceptionally(new RuntimeException("canceled"));
        }

        private void save() {
            long end = System.currentTimeMillis();
            long diff = end - start;
            queue.add(diff);
            logger.info("time : {} seq : {}", diff, sequence);
        }
    }
}
