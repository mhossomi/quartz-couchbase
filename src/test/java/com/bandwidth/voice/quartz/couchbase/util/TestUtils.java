package com.bandwidth.voice.quartz.couchbase.util;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class TestUtils {

    public static Runnable silence(ThrowingRunnable task) {
        return () -> {
            try {
                task.run();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static CompletableFuture<CountResult> count(int count, Executor executor,
            IntFunction<ThrowingRunnable> runner) {
        CountResult result = new CountResult();
        var futures = IntStream.range(0, count)
                .mapToObj(i -> silence(runner.apply(i)))
                .map(task -> runAsync(task, executor)
                        .handle((x, e) -> e == null)
                        .thenAccept(result::count))
                .toArray(CompletableFuture[]::new);

        return allOf(futures).thenApply(x -> result);
    }

    @Data
    public static class CountResult {
        public final AtomicInteger passCount = new AtomicInteger(0);
        public final AtomicInteger failCount = new AtomicInteger(0);

        private void count(boolean success) {
            if (success) {
                passCount.incrementAndGet();
            }
            else {
                failCount.incrementAndGet();
            }
        }

        private void count(CountResult result) {
            passCount.addAndGet(result.getPassCount());
            failCount.addAndGet(result.getFailCount());
        }

        public int getPassCount() {
            return passCount.get();
        }

        public int getFailCount() {
            return failCount.get();
        }
    }
}
