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

import io.trino.execution.TaskId;

import java.io.Closeable;

/**
 * Abstraction for polling pages from a buffer source.
 * Can be implemented for different transport mechanisms (HTTP, local memory, etc.)
 */
public interface PageBufferPoller
        extends Closeable
{
    /**
     * Schedule a request to fetch more pages from the remote buffer.
     * This method is called by DirectExchangeClient when it needs more data.
     */
    void scheduleRequest();

    /**
     * Get the current status of this poller for monitoring/debugging.
     */
    PageBufferClientStatus getStatus();

    /**
     * Get the average size in bytes of requests made by this poller.
     * Used by DirectExchangeClient for scheduling decisions.
     */
    long getAverageRequestSizeInBytes();

    /**
     * Get the duration in milliseconds of the last completed request.
     * Used for statistics collection.
     */
    long getLastRequestDurationMillis();

    /**
     * Get the remote task ID this poller is fetching data from.
     */
    TaskId getRemoteTaskId();

    /**
     * Close this poller and release any resources.
     */
    @Override
    void close();
}
