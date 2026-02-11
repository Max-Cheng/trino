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
package io.trino.operator.pagebuffer.local;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.SqlTaskManager.SqlTaskWithResults;
import io.trino.execution.TaskId;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.operator.PageBufferClientStatus;
import io.trino.operator.pagebuffer.PageBufferCallBack;
import io.trino.operator.pagebuffer.PageBufferResolver;
import io.trino.server.remotetask.Backoff;
import io.trino.spi.TrinoException;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.execution.buffer.PagesSerdeUtil.getSerializedPagePositionCount;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class LocalPageBuffer
        implements PageBufferResolver, AutoCloseable
{
    private static final Logger log = Logger.get(LocalPageBuffer.class);
    private static final Pattern BUFFER_ID_PATTERN = Pattern.compile(".*/results/([^/]+)(?:/.*)?$");

    private final SqlTaskManager taskManager;
    private final DataSize maxResponseSize;
    private final TaskId remoteTaskId;
    private final URI location;
    private final PipelinedOutputBuffers.OutputBufferId bufferId;
    private final PageBufferCallBack clientCallback;
    private final ScheduledExecutorService scheduledExecutor;
    private final Backoff backoff;

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
    @GuardedBy("this")
    private boolean requestInProgress;

    private volatile long lastRequestStartNanos;
    private volatile long lastRequestDurationMillis;
    private volatile long averageRequestSizeInBytes;

    private final AtomicLong rowsReceived = new AtomicLong();
    private final AtomicInteger pagesReceived = new AtomicInteger();

    private final AtomicLong rowsRejected = new AtomicLong();
    private final AtomicInteger pagesRejected = new AtomicInteger();

    private final AtomicInteger requestsScheduled = new AtomicInteger();
    private final AtomicInteger requestsCompleted = new AtomicInteger();
    private final AtomicInteger requestsSucceeded = new AtomicInteger();
    private final AtomicInteger requestsFailed = new AtomicInteger();

    private final Executor pageBufferClientCallbackExecutor;
    private final Ticker ticker;

    public LocalPageBuffer(
            SqlTaskManager taskManager,
            DataSize maxResponseSize,
            Duration maxErrorDuration,
            TaskId remoteTaskId,
            URI location,
            PageBufferCallBack clientCallback,
            ScheduledExecutorService scheduledExecutor,
            Executor pageBufferClientCallbackExecutor)
    {
        this(
                taskManager,
                maxResponseSize,
                maxErrorDuration,
                remoteTaskId,
                location,
                clientCallback,
                scheduledExecutor,
                Ticker.systemTicker(),
                pageBufferClientCallbackExecutor);
    }

    public LocalPageBuffer(
            SqlTaskManager taskManager,
            DataSize maxResponseSize,
            Duration maxErrorDuration,
            TaskId remoteTaskId,
            URI location,
            PageBufferCallBack clientCallback,
            ScheduledExecutorService scheduledExecutor,
            Ticker ticker,
            Executor pageBufferClientCallbackExecutor)
    {
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.maxResponseSize = requireNonNull(maxResponseSize, "maxResponseSize is null");
        this.remoteTaskId = requireNonNull(remoteTaskId, "remoteTaskId is null");
        this.location = requireNonNull(location, "location is null");
        this.bufferId = extractBufferId(location);
        this.clientCallback = requireNonNull(clientCallback, "clientCallback is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.pageBufferClientCallbackExecutor = requireNonNull(pageBufferClientCallbackExecutor, "pageBufferClientCallbackExecutor is null");
        requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        requireNonNull(ticker, "ticker is null");
        this.backoff = new Backoff(maxErrorDuration, ticker);
        this.ticker = ticker;
    }

    private static PipelinedOutputBuffers.OutputBufferId extractBufferId(URI location)
    {
        Matcher matcher = BUFFER_ID_PATTERN.matcher(location.getPath());
        if (matcher.find()) {
            return PipelinedOutputBuffers.OutputBufferId.fromString(matcher.group(1));
        }
        throw new IllegalArgumentException("Cannot extract buffer ID from location: " + location);
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
        else if (scheduled) {
            state = "scheduled";
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
                location,
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
                "local");
    }

    @Override
    public TaskId getRemoteTaskId()
    {
        return remoteTaskId;
    }

    @Override
    public long getAverageRequestSizeInBytes()
    {
        return averageRequestSizeInBytes;
    }

    @Override
    public synchronized void scheduleRequest()
    {
        if (closed || requestInProgress || scheduled) {
            return;
        }
        scheduled = true;

        backoff.startRequest();

        long delayNanos = backoff.getBackoffDelayNanos();
        scheduledExecutor.schedule(() -> {
            try {
                initiateRequest();
            }
            catch (Throwable t) {
                assertNotHoldsLock(LocalPageBuffer.this);
                clientCallback.clientFailed(LocalPageBuffer.this, t);
            }
        }, delayNanos, NANOSECONDS);

        lastUpdate = Instant.now();
        requestsScheduled.incrementAndGet();
    }

    @Override
    public double getLastRequestDurationMillis()
    {
        return lastRequestDurationMillis;
    }

    private synchronized void initiateRequest()
    {
        scheduled = false;
        if (closed || requestInProgress) {
            return;
        }

        if (completed) {
            destroyTaskResults();
        }
        else {
            sendGetResults();
        }

        lastUpdate = Instant.now();
    }

    private synchronized void sendGetResults()
    {
        requestInProgress = true;
        lastRequestStartNanos = ticker.read();

        SqlTaskWithResults taskWithResults = taskManager.getTaskResults(remoteTaskId, bufferId, token, maxResponseSize);
        taskWithResults.recordHeartbeat();

        Futures.addCallback(taskWithResults.getResultsFuture(), new FutureCallback<>()
        {
            @Override
            public void onSuccess(BufferResult result)
            {
                assertNotHoldsLock(LocalPageBuffer.this);
                lastRequestDurationMillis = (ticker.read() - lastRequestStartNanos) / 1_000_000;
                backoff.success();

                List<Slice> pages;
                boolean pagesAccepted;
                try {
                    synchronized (LocalPageBuffer.this) {
                        if (result.token() == token) {
                            pages = result.serializedPages();
                            token = result.nextToken();
                        }
                        else {
                            pages = List.of();
                        }
                    }

                    pagesAccepted = clientCallback.addPages(LocalPageBuffer.this, pages);
                }
                catch (TrinoException e) {
                    handleFailure(e);
                    return;
                }

                // update client stats
                if (!pages.isEmpty()) {
                    int pageCount = pages.size();
                    long rowCount = pages.stream().mapToLong(page -> getSerializedPagePositionCount(page)).sum();
                    if (pagesAccepted) {
                        pagesReceived.addAndGet(pageCount);
                        rowsReceived.addAndGet(rowCount);
                    }
                    else {
                        pagesRejected.addAndGet(pageCount);
                        rowsRejected.addAndGet(rowCount);
                    }
                }
                requestsCompleted.incrementAndGet();
                long responseSize = pages.stream().mapToLong(Slice::length).sum();
                requestSucceeded(responseSize);

                synchronized (LocalPageBuffer.this) {
                    if (result.bufferComplete()) {
                        completed = true;
                    }
                    requestInProgress = false;
                    lastUpdate = Instant.now();
                }
                clientCallback.requestComplete(LocalPageBuffer.this);
            }

            @Override
            public void onFailure(Throwable t)
            {
                log.debug("Local request to %s failed %s", location, t);
                assertNotHoldsLock(LocalPageBuffer.this);

                lastRequestDurationMillis = (ticker.read() - lastRequestStartNanos) / 1_000_000;

                if (!(t instanceof TrinoException) && backoff.failure()) {
                    String message = format("Error fetching local task results (%s - %s failures)", location, backoff.getFailureCount());
                    t = new TrinoException(REMOTE_TASK_FAILED, message, t);
                }
                handleFailure(t);
            }
        }, pageBufferClientCallbackExecutor);
    }

    private synchronized void requestSucceeded(long responseSize)
    {
        int successfulRequests = requestsSucceeded.incrementAndGet();
        averageRequestSizeInBytes = (long) ((1.0 * averageRequestSizeInBytes * (successfulRequests - 1)) + responseSize) / successfulRequests;
    }

    private synchronized void destroyTaskResults()
    {
        requestInProgress = true;
        try {
            taskManager.destroyTaskResults(remoteTaskId, bufferId);
            backoff.success();
            synchronized (this) {
                closed = true;
                requestInProgress = false;
                lastUpdate = Instant.now();
            }
            requestsCompleted.incrementAndGet();
            clientCallback.clientFinished(LocalPageBuffer.this);
        }
        catch (Throwable t) {
            handleFailure(t);
        }
    }

    @SuppressWarnings("checkstyle:IllegalToken")
    private static void assertNotHoldsLock(Object lock)
    {
        assert !Thread.holdsLock(lock) : "Cannot execute this method while holding a lock";
    }

    private void handleFailure(Throwable t)
    {
        assertNotHoldsLock(LocalPageBuffer.this);

        requestsFailed.incrementAndGet();
        requestsCompleted.incrementAndGet();

        if (t instanceof TrinoException) {
            clientCallback.clientFailed(LocalPageBuffer.this, t);
        }

        synchronized (LocalPageBuffer.this) {
            requestInProgress = false;
            lastUpdate = Instant.now();
        }
        clientCallback.requestComplete(LocalPageBuffer.this);
    }

    @Override
    public void close()
    {
        boolean shouldDestroyTaskResults;
        synchronized (this) {
            shouldDestroyTaskResults = !closed;
            closed = true;
            lastUpdate = Instant.now();
        }

        if (shouldDestroyTaskResults) {
            destroyTaskResults();
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LocalPageBuffer that = (LocalPageBuffer) o;

        return location.equals(that.location);
    }

    @Override
    public int hashCode()
    {
        return location.hashCode();
    }

    @Override
    public String toString()
    {
        String state;
        synchronized (this) {
            if (closed) {
                state = "CLOSED";
            }
            else if (requestInProgress) {
                state = "RUNNING";
            }
            else {
                state = "QUEUED";
            }
        }
        return "LocalPageBuffer{" +
                "location=" + location +
                ", state=" + state +
                '}';
    }
}
