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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.SqlTaskManager.SqlTaskWithResults;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.spi.TrinoException;

import java.io.Closeable;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Optimized page buffer client that directly accesses local TaskManager
 * instead of going through HTTP. This avoids serialization/deserialization
 * and network overhead when source and destination tasks are on the same node.
 *
 * Note: This class provides the same interface as HttpPageBufferClient but
 * accesses data locally.
 */
@ThreadSafe
public class LocalOptimizedPageBufferClient
        implements Closeable
{
    private static final Logger log = Logger.get(LocalOptimizedPageBufferClient.class);

    private final SqlTaskManager taskManager;
    private final TaskId remoteTaskId;
    private final PipelinedOutputBuffers.OutputBufferId bufferId;
    private final URI location;
    private final DataSize maxResponseSize;
    private final HttpPageBufferClient.ClientCallback clientCallback;
    private final ScheduledExecutorService scheduler;
    private final Executor callbackExecutor;
    private final String selfAddress;

    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private Instant lastUpdate = Instant.now();
    @GuardedBy("this")
    private long token;
    @GuardedBy("this")
    private boolean scheduled;
    @GuardedBy("this")
    private boolean completed;

    private final AtomicLong rowsReceived = new AtomicLong();
    private final AtomicInteger pagesReceived = new AtomicInteger();
    private final AtomicInteger requestsScheduled = new AtomicInteger();
    private final AtomicInteger requestsCompleted = new AtomicInteger();
    private final AtomicInteger requestsSucceeded = new AtomicInteger();
    private final AtomicInteger requestsFailed = new AtomicInteger();
    private volatile long averageRequestSizeInBytes;
    private volatile long lastRequestDurationMillis;

    public LocalOptimizedPageBufferClient(
            String selfAddress,
            SqlTaskManager taskManager,
            TaskId remoteTaskId,
            PipelinedOutputBuffers.OutputBufferId bufferId,
            URI location,
            DataSize maxResponseSize,
            HttpPageBufferClient.ClientCallback clientCallback,
            ScheduledExecutorService scheduler,
            Executor callbackExecutor)
    {
        this.selfAddress = requireNonNull(selfAddress, "selfAddress is null");
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.remoteTaskId = requireNonNull(remoteTaskId, "remoteTaskId is null");
        this.bufferId = requireNonNull(bufferId, "bufferId is null");
        this.location = requireNonNull(location, "location is null");
        this.maxResponseSize = requireNonNull(maxResponseSize, "maxResponseSize is null");
        this.clientCallback = requireNonNull(clientCallback, "clientCallback is null");
        this.scheduler = requireNonNull(scheduler, "scheduler is null");
        this.callbackExecutor = requireNonNull(callbackExecutor, "callbackExecutor is null");

        log.info("Created local optimized client for task %s buffer %s (avoiding HTTP overhead)", remoteTaskId, bufferId);
    }

    public TaskId getRemoteTaskId()
    {
        return remoteTaskId;
    }

    public long getAverageRequestSizeInBytes()
    {
        return averageRequestSizeInBytes;
    }

    public long getLastRequestDurationMillis()
    {
        return lastRequestDurationMillis;
    }

    public synchronized boolean isRunning()
    {
        return scheduled;
    }

    public synchronized void scheduleRequest()
    {
        if (closed || scheduled) {
            return;
        }
        scheduled = true;
        lastUpdate = Instant.now();
        requestsScheduled.incrementAndGet();

        // Schedule local request with minimal delay
        scheduler.schedule(() -> {
            try {
                executeLocalRequest();
            }
            catch (Throwable t) {
                log.error(t, "Local request failed for %s", location);
                handleFailure(t);
            }
        }, 1, TimeUnit.MILLISECONDS);
    }

    private void executeLocalRequest()
    {
        long requestStartMillis = System.currentTimeMillis();
        synchronized (this) {
            scheduled = false;
            if (closed || completed) {
                return;
            }
        }

        try {
            // Direct local access to TaskManager
            SqlTaskWithResults taskWithResults = taskManager.getTaskResults(
                    remoteTaskId,
                    bufferId,
                    token,
                    maxResponseSize);

            ListenableFuture<BufferResult> resultFuture = taskWithResults.getResultsFuture();

            // Add callback to process results
            addSuccessCallback(resultFuture, result -> {
                lastRequestDurationMillis = System.currentTimeMillis() - requestStartMillis;
                try {
                    // Check if task failed
                    if (taskWithResults.isTaskFailedOrFailing()) {
                        throw new TrinoException(REMOTE_TASK_FAILED, format("Remote task failed: %s", remoteTaskId));
                    }

                    processResult(result);
                }
                catch (Exception e) {
                    log.error(e, "Error processing local result for %s", location);
                    handleFailure(e);
                }
            }, callbackExecutor);
        }
        catch (Exception e) {
            log.error(e, "Error executing local request for %s", location);
            handleFailure(e);
        }
    }

    private synchronized void processResult(BufferResult result)
    {
        if (closed) {
            return;
        }

        lastUpdate = Instant.now();
        requestsCompleted.incrementAndGet();

        try {
            // Get pages directly (no deserialization needed for local access!)
            List<Slice> pages = result.serializedPages();

            // Update token
            token = result.nextToken();

            // Check if completed
            if (result.bufferComplete()) {
                completed = true;
            }

            // Add pages to client callback - need to cast since callback expects HttpPageBufferClient
            // but we're LocalOptimizedPageBufferClient
            @SuppressWarnings("unchecked")
            HttpPageBufferClient.ClientCallback callback = (HttpPageBufferClient.ClientCallback) clientCallback;
            boolean accepted = callback.addPages((HttpPageBufferClient) (Object) this, pages);

            if (accepted) {
                long responseSize = pages.stream().mapToLong(Slice::length).sum();
                requestSucceeded(responseSize);
                pagesReceived.addAndGet(pages.size());
                rowsReceived.addAndGet(responseSize);
            }

            callback.requestComplete((HttpPageBufferClient) (Object) this);

            if (result.bufferComplete()) {
                callback.clientFinished((HttpPageBufferClient) (Object) this);
            }
        }
        catch (Exception e) {
            handleFailure(e);
        }
    }

    private synchronized void handleFailure(Throwable t)
    {
        if (closed) {
            return;
        }

        requestsCompleted.incrementAndGet();
        requestsFailed.incrementAndGet();

        log.error(t, "Local request to %s failed", location);
        @SuppressWarnings("unchecked")
        HttpPageBufferClient.ClientCallback callback = (HttpPageBufferClient.ClientCallback) clientCallback;
        callback.clientFailed((HttpPageBufferClient) (Object) this, t);
    }

    private synchronized void requestSucceeded(long responseSize)
    {
        int successfulRequests = requestsSucceeded.incrementAndGet();
        // AVG_n = AVG_(n-1) * (n-1)/n + VALUE_n / n
        averageRequestSizeInBytes = (long) ((1.0 * averageRequestSizeInBytes * (successfulRequests - 1)) + responseSize) / successfulRequests;
    }

    public void close()
    {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }

        // Clean up local resources if needed
        try {
            taskManager.destroyTaskResults(remoteTaskId, bufferId);
        }
        catch (Exception e) {
            log.warn(e, "Error destroying task results for %s", remoteTaskId);
        }
    }

    public synchronized PageBufferClientStatus getStatus()
    {
        String state;
        if (closed) {
            state = "closed";
        }
        else if (scheduled) {
            state = "scheduled-local";
        }
        else if (completed) {
            state = "completed";
        }
        else {
            state = "queued";
        }

        return new PageBufferClientStatus(
                location,
                state,
                lastUpdate,
                rowsReceived.get(),
                pagesReceived.get(),
                OptionalLong.empty(), // rejectedRows
                OptionalInt.empty(), // rejectedPages
                requestsScheduled.get(),
                requestsCompleted.get(),
                requestsFailed.get(),
                requestsSucceeded.get(),
                "local-optimized");
    }
}
