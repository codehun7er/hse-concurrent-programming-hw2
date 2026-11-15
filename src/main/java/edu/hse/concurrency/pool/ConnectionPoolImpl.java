package edu.hse.concurrency.pool;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionPoolImpl implements ConnectionPool {

    private static final String STOPPED = "Пул остановлен";

    private final ConnectionFactory factory;
    private final long idleTimeoutNanos;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition idleChanged = lock.newCondition();
    private final Condition allReturned = lock.newCondition();
    private final ArrayDeque<IdleConnection> idle = new ArrayDeque<>();
    private final Set<Lease> leases = new HashSet<>();
    private final ArrayDeque<Ticket> waiters = new ArrayDeque<>();
    private final Thread evictor;

    private PoolConfig config;
    private int creating;
    private boolean stopped;

    public ConnectionPoolImpl(PoolConfig config, ConnectionFactory factory) {
        this.config = Objects.requireNonNull(config, "config");
        this.factory = Objects.requireNonNull(factory, "factory");
        this.idleTimeoutNanos = saturatedNanos(config.idleTimeout());
        this.evictor = new Thread(this::evictExpired, "connection-pool-evictor");
        evictor.setDaemon(true);
        evictor.start();
    }

    @Override
    public PooledConnection borrow(Duration timeout)
            throws InterruptedException, PoolTimeoutException, ConnectionCreationException {
        long nanos = nonNegativeNanos(timeout, "timeout");
        Ticket ticket = new Ticket(lock.newCondition());
        lock.lock();
        try {
            ensureRunning();
            if (!(waiters.isEmpty() && serve(ticket))) {
                awaitTurn(ticket, timeout, nanos);
            }
        } finally {
            lock.unlock();
            closeQuietly(ticket.discarded);
        }
        return ticket.lease != null ? ticket.lease : create();
    }

    @Override
    public void resize(int maxSize) {
        List<RawConnection> excess = new ArrayList<>();
        lock.lock();
        try {
            PoolConfig resized = config.withMaxSize(maxSize);
            ensureRunning();
            config = resized;
            while (total() > maxSize && !idle.isEmpty()) {
                excess.add(idle.pollLast().raw());
            }
            dispatch();
        } finally {
            lock.unlock();
        }
        excess.forEach(ConnectionPoolImpl::closeQuietly);
    }

    @Override
    public PoolConfig config() {
        lock.lock();
        try {
            return config;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PoolStats stats() {
        lock.lock();
        try {
            return new PoolStats(total(), idle.size(), leases.size(), creating, waiters.size());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown(Duration grace) throws InterruptedException {
        long nanos = nonNegativeNanos(grace, "grace");
        List<RawConnection> idleConnections = new ArrayList<>();
        lock.lock();
        try {
            if (stopped) {
                return;
            }
            stopped = true;
            waiters.forEach(waiter -> waiter.turn.signal());
            waiters.clear();
            idle.forEach(connection -> idleConnections.add(connection.raw()));
            idle.clear();
            idleChanged.signal();
        } finally {
            lock.unlock();
        }
        idleConnections.forEach(ConnectionPoolImpl::closeQuietly);

        InterruptedException interrupted = null;
        List<RawConnection> forced = new ArrayList<>();
        lock.lock();
        try {
            while (!leases.isEmpty() && nanos > 0) {
                nanos = allReturned.awaitNanos(nanos);
            }
        } catch (InterruptedException e) {
            interrupted = e;
        } finally {
            leases.forEach(lease -> {
                lease.released = true;
                forced.add(lease.raw);
            });
            leases.clear();
            lock.unlock();
        }
        forced.forEach(ConnectionPoolImpl::closeQuietly);

        joinEvictor();
        if (interrupted != null) {
            throw interrupted;
        }
    }

    private void awaitTurn(Ticket ticket, Duration timeout, long nanos)
            throws InterruptedException, PoolTimeoutException {
        waiters.addLast(ticket);
        try {
            while (!ticket.served()) {
                ensureRunning();
                if (nanos <= 0) {
                    waiters.remove(ticket);
                    throw new PoolTimeoutException(timeout);
                }
                nanos = ticket.turn.awaitNanos(nanos);
            }
            ensureRunning();
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        } catch (InterruptedException | IllegalStateException e) {
            waiters.remove(ticket);
            refund(ticket);
            throw e;
        }
    }

    private void refund(Ticket ticket) {
        if (ticket.slotReserved) {
            ticket.slotReserved = false;
            releaseSlot();
        } else if (ticket.lease != null && !ticket.lease.released) {
            ticket.discarded = reclaim(ticket.lease);
        }
    }

    private Lease create() throws ConnectionCreationException {
        RawConnection raw;
        try {
            raw = factory.create();
        } catch (Exception e) {
            lock.lock();
            try {
                releaseSlot();
            } finally {
                lock.unlock();
            }
            throw new ConnectionCreationException(e);
        }
        lock.lock();
        try {
            creating--;
            if (!stopped) {
                return lease(raw);
            }
        } finally {
            lock.unlock();
        }
        closeQuietly(raw);
        throw new IllegalStateException(STOPPED);
    }

    private void giveBack(Lease lease) {
        RawConnection discarded = null;
        lock.lock();
        try {
            if (!lease.released) {
                discarded = reclaim(lease);
            }
        } finally {
            lock.unlock();
        }
        closeQuietly(discarded);
    }

    private RawConnection reclaim(Lease lease) {
        lease.released = true;
        leases.remove(lease);
        if (stopped) {
            if (leases.isEmpty()) {
                allReturned.signalAll();
            }
            return lease.raw;
        }
        if (total() >= config.maxSize()) {
            return lease.raw;
        }
        idle.addFirst(new IdleConnection(lease.raw, System.nanoTime()));
        dispatch();
        if (idle.size() == config.minIdle() + 1) {
            idleChanged.signal();
        }
        return null;
    }

    private void releaseSlot() {
        creating--;
        dispatch();
    }

    private void evictExpired() {
        List<RawConnection> expired;
        while ((expired = awaitExpired()) != null) {
            expired.forEach(ConnectionPoolImpl::closeQuietly);
        }
    }

    private List<RawConnection> awaitExpired() {
        lock.lock();
        try {
            while (!stopped) {
                long now = System.nanoTime();
                List<RawConnection> expired = new ArrayList<>();
                while (idle.size() > config.minIdle() && now - idle.peekLast().since() >= idleTimeoutNanos) {
                    expired.add(idle.pollLast().raw());
                }
                if (!expired.isEmpty()) {
                    return expired;
                }
                if (idle.size() > config.minIdle()) {
                    idleChanged.awaitNanos(idleTimeoutNanos - (now - idle.peekLast().since()));
                } else {
                    idleChanged.await();
                }
            }
            return null;
        } catch (InterruptedException e) {
            return null;
        } finally {
            lock.unlock();
        }
    }

    private void joinEvictor() {
        boolean interrupted = false;
        while (evictor.isAlive()) {
            try {
                evictor.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean serve(Ticket ticket) {
        if (!idle.isEmpty()) {
            ticket.lease = lease(idle.pollFirst().raw());
        } else if (total() < config.maxSize()) {
            creating++;
            ticket.slotReserved = true;
        } else {
            return false;
        }
        return true;
    }

    private void dispatch() {
        while (!waiters.isEmpty() && serve(waiters.peekFirst())) {
            waiters.pollFirst().turn.signal();
        }
    }

    private Lease lease(RawConnection raw) {
        Lease lease = new Lease(raw);
        leases.add(lease);
        return lease;
    }

    private int total() {
        return idle.size() + leases.size() + creating;
    }

    private void ensureRunning() {
        if (stopped) {
            throw new IllegalStateException(STOPPED);
        }
    }

    private static void closeQuietly(RawConnection raw) {
        if (raw == null) {
            return;
        }
        try {
            raw.close();
        } catch (Exception ignored) {
        }
    }

    private static long nonNegativeNanos(Duration duration, String name) {
        Objects.requireNonNull(duration, name);
        if (duration.isNegative()) {
            throw new IllegalArgumentException(name + " не может быть отрицательным: " + duration);
        }
        return saturatedNanos(duration);
    }

    private static long saturatedNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException tooLong) {
            return Long.MAX_VALUE;
        }
    }

    private record IdleConnection(RawConnection raw, long since) {
    }

    private static final class Ticket {

        final Condition turn;
        Lease lease;
        boolean slotReserved;
        RawConnection discarded;

        Ticket(Condition turn) {
            this.turn = turn;
        }

        boolean served() {
            return lease != null || slotReserved;
        }
    }

    private final class Lease implements PooledConnection {

        final RawConnection raw;
        volatile boolean released;

        Lease(RawConnection raw) {
            this.raw = raw;
        }

        @Override
        public RawConnection raw() {
            if (released) {
                throw new IllegalStateException("Соединение уже возвращено или закрыто пулом");
            }
            return raw;
        }

        @Override
        public void close() {
            giveBack(this);
        }
    }
}
