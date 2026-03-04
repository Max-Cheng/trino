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

import io.airlift.slice.Slice;

import java.util.List;

/**
 * For each request, the addPage method will be called zero or more times,
 * followed by either requestComplete or clientFinished (if buffer complete).  If the client is
 * closed, requestComplete or bufferFinished may never be called.
 * <p>
 * <b>NOTE:</b> Implementations of this interface are not allowed to perform
 * blocking operations.
 */
public interface PageBufferCallBack
{
    boolean addPages(PageBufferPoller poller, List<Slice> pages);

    void requestComplete(PageBufferPoller poller);

    void clientFinished(PageBufferPoller poller);

    void clientFailed(PageBufferPoller poller, Throwable cause);
}
