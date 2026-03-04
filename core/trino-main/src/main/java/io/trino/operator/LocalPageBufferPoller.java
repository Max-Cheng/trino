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
import io.trino.execution.TaskId;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;

import java.time.Instant;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * LocalPageBufferPoller retrieves pages from a local task's output buffer
 * without going through HTTP. This is used for same-node task communication
 * to bypass the network stack and improve performance.
 */
@ThreadSafe
public class LocalPageBufferPoller
        implements PageBufferPoller
{
    private static final Logger log = Logger.get(LocalPageBufferPoller.class);

    private final SqlTaskManager sqlTaskManager;
    private final TaskId taskId;
    private final OutputBufferId bufferId;
    private final PageBufferCallBack clientCallback;
    private final DataSize maxResponseSize;
    private final boolean acknowledgePages;
    private final Executor executor;

    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private long token;
    @GuardedBy("this")
    private boolean requestInProgress;
    @GuardedBy("this")
    private boolean completed;
    @GuardedBy("this")
    private Instant lastUpdate = Instant.now();

    private final AtomicLong rowsReceived = new AtomicLong();
    private final AtomicInteger pagesReceived = new AtomicInteger();
    private final AtomicLong rowsRejected = new AtomicLong();
    private final AtomicInteger pagesRejected = new AtomicInteger();
    private final AtomicInteger requestsScheduled = new AtomicInteger();
    private final AtomicInteger requestsCompleted = new AtomicInteger();
    private final AtomicInteger requestsSucceeded = new AtomicInteger();
    private final AtomicInteger requestsFailed = new AtomicInteger();

    private volatile long lastRequestStartNanos;
    private volatile long lastRequestDurationMillis;
    private volatile long averageRequestSizeInBytes;

    public LocalPageBufferPoller(
            SqlTaskManager sqlTaskManager,
            TaskId taskId,
            int bufferId,
            DataSize maxResponseSize,
            boolean acknowledgePages,
            PageBufferCallBack clientCallback,
            Executor executor)
    {
        this.sqlTaskManager = requireNonNull(sqlTaskManager, "sqlTaskManager is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.bufferId = new OutputBufferId(bufferId);
        this.maxResponseSize = requireNonNull(maxResponseSize, "maxResponseSize is null");
        this.acknowledgePages = acknowledgePages;
        this.clientCallback = requireNonNull(clientCallback, "clientCallback is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public synchronized PageBufferClientStatus getStatus()
    {
        String state;
        if (closed) {
            state = "closed";
        }
        else if (requestInProgress) {
            state = "running";
        }
        else if (completed) {
            state = "completed";
        }
        else {
            state = "queued";
        }

        long rejectedRows = rowsRejected.get();
        int rejectedPages = pagesRejected.get();

        return new PageBufferClientStatus(
                null, // No URI for local poller
                state,
                lastUpdate,
                rowsReceived.get(),
                pagesReceived.get(),
                rejectedRows == 0 ? OptionalLong.empty() : OptionalLong.of(rejectedRows),
                rejectedPages == 0 ? OptionalInt.empty() : OptionalInt.of(rejectedPages),
                requestsScheduled.get(),
                requestsCompleted.get(),
                requestsFailed.get(),
                requestsSucceeded.get(),
                state);
    }

    @Override
    public long getAverageRequestSizeInBytes()
    {
        return averageRequestSizeInBytes;
    }

    @Override
    public long getLastRequestDurationMillis()
    {
        return lastRequestDurationMillis;
    }

    @Override
    public TaskId getRemoteTaskId()
    {
        return taskId;
    }

    @Override
    public void scheduleRequest()
    {
        synchronized (this) {
            if (closed || requestInProgress) {
                return;
            }
            requestInProgress = true;
            lastUpdate = Instant.now();
        }

        requestsScheduled.incrementAndGet();

        // Execute asynchronously to avoid blocking DirectExchangeClient's scheduling thread
        executor.execute(() -> {
            try {
                executeRequest();
            }
            catch (Throwable t) {
                // Should not happen, but be safe and fail the operator
                log.error(t, "Unexpected error in LocalPageBufferPoller for task %s", taskId);
                clientCallback.clientFailed(LocalPageBufferPoller.this, t);
            }
        });
    }

    private void executeRequest()
    {
        long startNanos = System.nanoTime();
        lastRequestStartNanos = startNanos;

        long currentToken;
        synchronized (this) {
            if (closed) {
                requestInProgress = false;
                return;
            }
            currentToken = token;

            if (completed) {
                // Already completed, just destroy results
                requestInProgress = false;
                destroyTaskResults();
                return;
            }
        }

        try {
            // Get results from local task manager
            SqlTaskManager.SqlTaskWithResults taskWithResults = sqlTaskManager.getTaskResults(
                    taskId,
                    bufferId,
                    currentToken,
                    maxResponseSize);

            taskWithResults.recordHeartbeat();
            ListenableFuture<BufferResult> resultsFuture = taskWithResults.getResultsFuture();

            // Add listener to handle the result asynchronously
            resultsFuture.addListener(() -> {
                try {
                    if (!resultsFuture.isDone() || resultsFuture.isCancelled()) {
                        return;
                    }

                    BufferResult result = resultsFuture.get();
                    handleBufferResult(result, startNanos);
                }
                catch (Exception e) {
                    log.error(e, "Failed to get buffer result for task %s", taskId);
                    requestsFailed.incrementAndGet();
                    clientCallback.clientFailed(LocalPageBufferPoller.this, e);
                }
                finally {
                    synchronized (LocalPageBufferPoller.this) {
                        requestInProgress = false;
                        lastUpdate = Instant.now();
                    }
                    requestsCompleted.incrementAndGet();
                    clientCallback.requestComplete(LocalPageBufferPoller.this);
                }
            }, executor);
        }
        catch (Exception e) {
            log.error(e, "Failed to initiate request for task %s", taskId);
            synchronized (this) {
                requestInProgress = false;
            }
            requestsFailed.incrementAndGet();
            requestsCompleted.incrementAndGet();
            clientCallback.clientFailed(this, e);
        }
    }

    private void handleBufferResult(BufferResult result, long startNanos)
    {
        lastRequestDurationMillis = (System.nanoTime() - startNanos) / 1_000_000;

        List<Slice> pages = result.serializedPages();
        long nextToken = result.nextToken();
        boolean bufferComplete = result.bufferComplete();

        // Acknowledge pages to free memory on the producer side
        if (!pages.isEmpty() && acknowledgePages) {
            try {
                sqlTaskManager.acknowledgeTaskResults(taskId, bufferId, nextToken);
            }
            catch (Exception e) {
                log.debug(e, "Failed to acknowledge pages for task %s", taskId);
                // Continue processing even if acknowledge fails
            }
        }

        // Update statistics
        if (!pages.isEmpty()) {
            long totalBytes = pages.stream().mapToLong(Slice::length).sum();
            pagesReceived.addAndGet(pages.size());

            // Update average request size
            long currentAverage = averageRequestSizeInBytes;
            long requestCount = requestsSucceeded.get() + 1;
            averageRequestSizeInBytes = (currentAverage * (requestCount - 1) + totalBytes) / requestCount;
        }

        // Notify callback
        boolean pagesAccepted = clientCallback.addPages(LocalPageBufferPoller.this, pages);

        if (!pagesAccepted && !pages.isEmpty()) {
            rowsRejected.addAndGet(pages.size());
            pagesRejected.addAndGet(pages.size());
        }
        else if (!pages.isEmpty()) {
            rowsReceived.addAndGet(pages.size());
        }

        // Update token
        synchronized (this) {
            token = nextToken;

            if (bufferComplete) {
                completed = true;
                clientCallback.clientFinished(LocalPageBufferPoller.this);
            }
        }

        requestsSucceeded.incrementAndGet();
    }

    @Override
    public void close()
    {
        boolean shouldDestroy;
        synchronized (this) {
            shouldDestroy = !closed;
            closed = true;
            lastUpdate = Instant.now();
        }

        if (shouldDestroy) {
            destroyTaskResults();
        }
    }

    private void destroyTaskResults()
    {
        try {
            sqlTaskManager.destroyTaskResults(taskId, bufferId);
        }
        catch (Exception e) {
            log.debug(e, "Failed to destroy task results for task %s", taskId);
        }
    }
}
