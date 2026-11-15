package edu.hse.concurrency.pool;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(90)
class PoolStarvationTest {

    private static final int THREADS = 8;
    private static final int CONNECTIONS_PER_THREAD = 2;
    private static final Duration SECOND_BORROW_TIMEOUT = Duration.ofMinutes(1);
    private static final Duration STALL_OBSERVATION = Duration.ofMillis(
            Long.parseLong(System.getenv().getOrDefault("STARVATION_HOLD_MILLIS", "500")));

    private ConnectionPool pool;

    @AfterEach
    void tearDown() throws InterruptedException {
        pool.shutdown(Duration.ZERO);
    }

    @Test
    void poolStallsWhenEveryThreadHoldsOneConnectionAndAsksForAnother() throws Exception {
        pool = poolOfSize(THREADS * (CONNECTIONS_PER_THREAD - 1));
        List<Request> requests = startRequests();

        PoolStats stalled = new PoolStats(THREADS, 0, THREADS, 0, THREADS);
        awaitStats(stalled);
        Thread.sleep(STALL_OBSERVATION);
        assertEquals(stalled, pool.stats());
        assertTrue(requests.stream().allMatch(request -> request.thread.isAlive()));

        pool.shutdown(Duration.ZERO);
        for (Request request : requests) {
            request.thread.join();
            assertInstanceOf(IllegalStateException.class, request.failure);
        }
    }

    @Test
    void allThreadsFinishWithMinimalSafeMaxSize() throws Exception {
        pool = poolOfSize(minimalSafeMaxSize(THREADS, CONNECTIONS_PER_THREAD));
        List<Request> requests = startRequests();

        for (Request request : requests) {
            request.thread.join();
            assertNull(request.failure);
        }
        assertEquals(0, pool.stats().borrowed());
    }

    private static int minimalSafeMaxSize(int threads, int connectionsPerThread) {
        return threads * (connectionsPerThread - 1) + 1;
    }

    private static ConnectionPool poolOfSize(int maxSize) {
        return new ConnectionPoolImpl(new PoolConfig(0, maxSize, Duration.ofMinutes(1), false), TestConnection::new);
    }

    private List<Request> startRequests() {
        CyclicBarrier everyoneHoldsAllButLast = new CyclicBarrier(THREADS);
        List<Request> requests = new ArrayList<>();
        for (int i = 1; i <= THREADS; i++) {
            Request request = new Request();
            request.thread = new Thread(() -> request.run(everyoneHoldsAllButLast), "request-" + i);
            requests.add(request);
            request.thread.start();
        }
        return requests;
    }

    private void awaitStats(PoolStats expected) throws InterruptedException {
        while (!expected.equals(pool.stats())) {
            Thread.sleep(10);
        }
    }

    private final class Request {

        Thread thread;
        Exception failure;

        void run(CyclicBarrier everyoneHoldsAllButLast) {
            List<PooledConnection> held = new ArrayList<>();
            try {
                for (int i = 1; i < CONNECTIONS_PER_THREAD; i++) {
                    held.add(pool.borrow(Duration.ofSeconds(5)));
                }
                everyoneHoldsAllButLast.await();
                held.add(pool.borrow(SECOND_BORROW_TIMEOUT));
            } catch (Exception e) {
                failure = e;
            } finally {
                held.forEach(PooledConnection::close);
            }
        }
    }

    private static final class TestConnection implements RawConnection {

        @Override
        public void close() {
        }
    }
}
