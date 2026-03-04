/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Provider;
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.stats.TDigest;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.FeaturesConfig.DataIntegrityVerification;
import io.trino.exchange.DirectExchangeInput;
import io.trino.exchange.ExchangeInput;
import io.trino.exchange.PassThroughExchangeInput;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.TaskFailureListener;
import io.trino.execution.TaskId;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.plugin.base.metrics.TDigestHistogram;
import jakarta.annotation.Nullable;

import java.io.Closeable;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DirectExchangeClient
        implements Closeable, PageBufferCallBack
{
    private static final Logger log = Logger.get(DirectExchangeClient.class);

    private final String selfAddress;
    private final DataIntegrityVerification dataIntegrityVerification;
    private final DataSize maxResponseSize;
    private final int concurrentRequestMultiplier;
    private final Duration maxErrorDuration;
    private final boolean acknowledgePages;
    private final HttpClient httpClient;
    private final ScheduledExecutorService scheduledExecutor;

    @GuardedBy("this")
    private boolean noMoreLocations;

    private final Map<URI, PageBufferPoller> allClients = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final Set<PageBufferPoller> queuedClients = new LinkedHashSet<>();
    @GuardedBy("this")
    private final Set<PageBufferPoller> runningClients = new LinkedHashSet<>();

    private final Set<PageBufferPoller> completedClients = newConcurrentHashSet();
    private final DirectExchangeBuffer buffer;

    @GuardedBy("this")
    private long successfulRequests;
    @GuardedBy("this")
    private long averageBytesPerRequest;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private final TDigest requestDuration = new TDigest();

    @GuardedBy("memoryContextLock")
    @Nullable
    private LocalMemoryContext memoryContext;
    private final ReadWriteLock memoryContextLock = new ReentrantReadWriteLock();
    private final Lock memoryContextReadLock = memoryContextLock.readLock();
    private final Lock memoryContextWriteLock = memoryContextLock.writeLock();
    private final Executor pageBufferClientCallbackExecutor;
    private final TaskFailureListener taskFailureListener;
    private final Provider<SqlTaskManager> sqlTaskManagerProvider;

    // DirectExchangeClientStatus.mergeWith assumes all clients have the same bufferCapacity.
    // Please change that method accordingly when this assumption becomes not true.
    public DirectExchangeClient(
            String selfAddress,
            DataIntegrityVerification dataIntegrityVerification,
            DirectExchangeBuffer buffer,
            DataSize maxResponseSize,
            int concurrentRequestMultiplier,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            HttpClient httpClient,
            ScheduledExecutorService scheduledExecutor,
            LocalMemoryContext memoryContext,
            Executor pageBufferClientCallbackExecutor,
            TaskFailureListener taskFailureListener,
            Provider<SqlTaskManager> sqlTaskManagerProvider)
    {
        this.selfAddress = requireNonNull(selfAddress, "selfAddress is null");
        this.dataIntegrityVerification = requireNonNull(dataIntegrityVerification, "dataIntegrityVerification is null");
        this.buffer = requireNonNull(buffer, "buffer is null");
        this.maxResponseSize = maxResponseSize;
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.maxErrorDuration = maxErrorDuration;
        this.acknowledgePages = acknowledgePages;
        this.httpClient = httpClient;
        this.scheduledExecutor = scheduledExecutor;
        this.memoryContext = memoryContext;
        this.pageBufferClientCallbackExecutor = requireNonNull(pageBufferClientCallbackExecutor, "pageBufferClientCallbackExecutor is null");
        this.taskFailureListener = requireNonNull(taskFailureListener, "taskFailureListener is null");
        this.sqlTaskManagerProvider = sqlTaskManagerProvider;
    }

    public DirectExchangeClientStatus getStatus()
    {
        // The stats created by this method is only for diagnostics.
        // It does not guarantee a consistent view between different exchange clients.
        // Guaranteeing a consistent view introduces significant lock contention.
        ImmutableList.Builder<PageBufferClientStatus> pageBufferClientStatusBuilder = ImmutableList.builder();
        for (PageBufferPoller client : allClients.values()) {
            pageBufferClientStatusBuilder.add(client.getStatus());
        }
        List<PageBufferClientStatus> pageBufferClientStatus = pageBufferClientStatusBuilder.build();
        synchronized (this) {
            return new DirectExchangeClientStatus(
                    buffer.getRetainedSizeInBytes(),
                    buffer.getMaxRetainedSizeInBytes(),
                    averageBytesPerRequest,
                    successfulRequests,
                    buffer.getBufferedPageCount(),
                    buffer.getSpilledPageCount(),
                    buffer.getSpilledBytes(),
                    noMoreLocations,
                    pageBufferClientStatus,
                    new TDigestHistogram(TDigest.copyOf(requestDuration)));
        }
    }

    public synchronized void addInput(ExchangeInput input)
    {
        requireNonNull(input, "input is null");

        // Ignore new locations after close
        // NOTE: this MUST happen before checking no more locations is checked
        if (closed) {
            return;
        }

        TaskId taskId;
        PageBufferPoller poller;
        URI location;

        switch (input) {
            case DirectExchangeInput directExchangeInput -> {
                taskId = directExchangeInput.getTaskId();
                location = URI.create(directExchangeInput.getLocation());
                checkArgument(!allClients.containsKey(location), "location already exist: %s", location);
                checkState(!noMoreLocations, "No more locations already set");
                buffer.addTask(taskId);
                poller = new HttpPageBufferPoller(
                        selfAddress,
                        httpClient,
                        dataIntegrityVerification,
                        maxResponseSize,
                        maxErrorDuration,
                        acknowledgePages,
                        taskId,
                        location,
                        this,
                        scheduledExecutor,
                        pageBufferClientCallbackExecutor);
            }
            case PassThroughExchangeInput passThroughExchangeInput -> {
                taskId = passThroughExchangeInput.getTaskId();
                int partitionId = passThroughExchangeInput.getPartitionId();
                // Use a synthetic URI for local exchange (not actually used for HTTP)
                location = URI.create(format("http://localhost/%s/%d", taskId, partitionId));
                checkArgument(!allClients.containsKey(location), "location already exist: %s", location);
                checkState(!noMoreLocations, "No more locations already set");
                buffer.addTask(taskId);
                poller = new LocalPageBufferPoller(
                        sqlTaskManagerProvider.get(),
                        taskId,
                        passThroughExchangeInput.getPartitionId(),
                        maxResponseSize,
                        acknowledgePages,
                        this,
                        pageBufferClientCallbackExecutor);
            }
            default -> throw new IllegalStateException("Unexpected Exchange input: " + input);
        }

        allClients.put(location, poller);
        queuedClients.add(poller);

        scheduleRequestIfNecessary();
    }

    public synchronized void noMoreLocations()
    {
        noMoreLocations = true;
        buffer.noMoreTasks();
        scheduleRequestIfNecessary();
    }

    public WorkProcessor<Slice> pages()
    {
        return WorkProcessor.create(() -> {
            Slice page = pollPage();
            if (page == null) {
                if (isFinished()) {
                    return ProcessState.finished();
                }

                ListenableFuture<Void> blocked = isBlocked();
                if (!blocked.isDone()) {
                    return ProcessState.blocked(blocked);
                }

                return ProcessState.yielded();
            }

            return ProcessState.ofResult(page);
        });
    }

    @SuppressWarnings("checkstyle:IllegalToken")
    private void assertNotHoldsLock()
    {
        assert !Thread.holdsLock(this) : "Cannot get next page while holding a lock on this";
    }

    @Nullable
    public Slice pollPage()
    {
        assertNotHoldsLock();

        Slice page = buffer.pollPage();

        if (page == null) {
            return null;
        }

        // updating retained memory might be expensive, therefore it needs to be updated outside of exclusive lock
        updateRetainedMemory();
        scheduleRequestIfNecessary();

        // Return the page even if the client is closed.
        // A concurrent thread may have responded to the `isFinished` change
        // triggered by polling this page and may have closed the client.
        return page;
    }

    public boolean isFinished()
    {
        return buffer.isFinished() && completedClients.size() == allClients.size();
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        for (PageBufferPoller client : allClients.values()) {
            closeQuietly(client);
        }
        try {
            buffer.close();
        }
        catch (RuntimeException e) {
            log.warn(e, "error closing buffer");
        }
        finally {
            releaseMemoryContext();
        }
    }

    @VisibleForTesting
    synchronized int scheduleRequestIfNecessary()
    {
        if ((buffer.isFinished() || buffer.isFailed()) && completedClients.size() == allClients.size()) {
            return 0;
        }

        long neededBytes = buffer.getRemainingCapacityInBytes();
        if (neededBytes <= 0) {
            return 0;
        }

        long reservedBytesForScheduledClients = runningClients.stream()
                .mapToLong(PageBufferPoller::getAverageRequestSizeInBytes)
                .sum();
        long projectedBytesToBeRequested = 0;
        int clientCount = 0;

        Iterator<PageBufferPoller> clientIterator = queuedClients.iterator();
        while (clientIterator.hasNext()) {
            PageBufferPoller client = clientIterator.next();
            if (projectedBytesToBeRequested >= neededBytes * concurrentRequestMultiplier - reservedBytesForScheduledClients) {
                break;
            }
            projectedBytesToBeRequested += client.getAverageRequestSizeInBytes();

            client.scheduleRequest();

            // Remove the client from the queuedClient's set.
            clientIterator.remove();
            runningClients.add(client);

            clientCount++;
        }

        return clientCount;
    }

    public ListenableFuture<Void> isBlocked()
    {
        return buffer.isBlocked();
    }

    @VisibleForTesting
    Set<PageBufferPoller> getQueuedClients()
    {
        return queuedClients;
    }

    @VisibleForTesting
    Set<PageBufferPoller> getRunningClients()
    {
        return runningClients;
    }

    @VisibleForTesting
    Map<URI, PageBufferPoller> getAllClients()
    {
        return allClients;
    }

    @Override
    public boolean addPages(PageBufferPoller poller, List<Slice> pages)
    {
        requireNonNull(poller, "poller is null");
        requireNonNull(pages, "pages is null");

        // If client is already completed, addPages is a no-op
        if (completedClients.contains(poller)) {
            return false;
        }

        // Compute stats before acquiring the lock
        long responseSize = 0;
        if (!pages.isEmpty()) {
            for (Slice page : pages) {
                responseSize += page.length();
            }
            // Buffer may already be closed at this point. In such situation the buffer is expected to simply ignore this call.
            buffer.addPages(poller.getRemoteTaskId(), pages);
            // updating retained memory might be expensive, therefore it needs to be updated outside of exclusive lock
            updateRetainedMemory();
        }

        synchronized (this) {
            if (closed || buffer.isFinished() || buffer.isFailed()) {
                return false;
            }

            successfulRequests++;
            // AVG_n = AVG_(n-1) * (n-1)/n + VALUE_n / n
            averageBytesPerRequest = (long) (1.0 * averageBytesPerRequest * (successfulRequests - 1) / successfulRequests + (double) responseSize / successfulRequests);
        }

        return true;
    }

    private void updateRetainedMemory()
    {
        memoryContextReadLock.lock();
        try {
            if (memoryContext != null) {
                memoryContext.setBytes(buffer.getRetainedSizeInBytes());
            }
        }
        finally {
            memoryContextReadLock.unlock();
        }
    }

    private void releaseMemoryContext()
    {
        memoryContextWriteLock.lock();
        try {
            if (memoryContext != null) {
                memoryContext.setBytes(0);
                // prevent further memory allocations
                memoryContext = null;
            }
        }
        finally {
            memoryContextWriteLock.unlock();
        }
    }

    @Override
    public synchronized void requestComplete(PageBufferPoller poller)
    {
        requireNonNull(poller, "poller is null");
        requestDuration.add(poller.getLastRequestDurationMillis());
        if (!completedClients.contains(poller) && !queuedClients.contains(poller)) {
            queuedClients.add(poller);
            runningClients.remove(poller);
        }
        scheduleRequestIfNecessary();
    }

    @Override
    public synchronized void clientFinished(PageBufferPoller poller)
    {
        requireNonNull(poller, "poller is null");
        if (completedClients.add(poller)) {
            runningClients.remove(poller);
            buffer.taskFinished(poller.getRemoteTaskId());
        }
        scheduleRequestIfNecessary();
    }

    @Override
    public synchronized void clientFailed(PageBufferPoller poller, Throwable cause)
    {
        requireNonNull(poller, "poller is null");
        requireNonNull(cause, "cause is null");
        if (completedClients.add(poller)) {
            runningClients.remove(poller);
            buffer.taskFailed(poller.getRemoteTaskId(), cause);
            scheduledExecutor.execute(() -> taskFailureListener.onTaskFailed(poller.getRemoteTaskId(), cause));
            closeQuietly(poller);
        }
        scheduleRequestIfNecessary();
    }

    private static void closeQuietly(PageBufferPoller poller)
    {
        try {
            poller.close();
        }
        catch (RuntimeException e) {
            // ignored
        }
    }
}
