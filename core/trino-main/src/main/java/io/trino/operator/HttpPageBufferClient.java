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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.slice.Slice;

import java.io.Closeable;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class HttpPageBufferClient
        implements Closeable
{
    /**
     * For each request, the addPage method will be called zero or more times,
     * followed by either requestComplete or clientFinished (if buffer complete).  If the client is
     * closed, requestComplete or bufferFinished may never be called.
     * <p>
     * <b>NOTE:</b> Implementations of this interface are not allowed to perform
     * blocking operations.
     */
    public interface ClientCallback
    {
        boolean addPages(HttpPageBufferClient client, List<Slice> pages);

        void requestComplete(HttpPageBufferClient client);

        void clientFinished(HttpPageBufferClient client);

        void clientFailed(HttpPageBufferClient client, Throwable cause);
    }

    public static class PagesResponse
    {
        public static PagesResponse createPagesResponse(String taskInstanceId, long token, long nextToken, Iterable<Slice> pages, boolean complete, boolean taskFailed)
        {
            return new PagesResponse(taskInstanceId, token, nextToken, pages, complete, taskFailed);
        }

        public static PagesResponse createEmptyPagesResponse(String taskInstanceId, long token, long nextToken, boolean complete, boolean taskFailed)
        {
            return new PagesResponse(taskInstanceId, token, nextToken, ImmutableList.of(), complete, taskFailed);
        }

        private final String taskInstanceId;
        private final long token;
        private final long nextToken;
        private final List<Slice> pages;
        private final boolean clientComplete;
        private final boolean taskFailed;

        private PagesResponse(String taskInstanceId, long token, long nextToken, Iterable<Slice> pages, boolean clientComplete, boolean taskFailed)
        {
            this.taskInstanceId = taskInstanceId;
            this.token = token;
            this.nextToken = nextToken;
            this.pages = ImmutableList.copyOf(pages);
            this.clientComplete = clientComplete;
            this.taskFailed = taskFailed;
        }

        public long getToken()
        {
            return token;
        }

        public long getNextToken()
        {
            return nextToken;
        }

        public List<Slice> getPages()
        {
            return pages;
        }

        public boolean isClientComplete()
        {
            return clientComplete;
        }

        public String getTaskInstanceId()
        {
            return taskInstanceId;
        }

        public boolean isTaskFailed()
        {
            return taskFailed;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("token", token)
                    .add("nextToken", nextToken)
                    .add("pagesSize", pages.size())
                    .add("clientComplete", clientComplete)
                    .add("taskFailed", taskFailed)
                    .toString();
        }
    }

    public static class ChecksumVerificationException
            extends RuntimeException
    {
        public ChecksumVerificationException(String message)
        {
            super(requireNonNull(message, "message is null"));
        }
    }

    private HttpPageBufferClient() {}

    @Override
    public void close() {}
}
