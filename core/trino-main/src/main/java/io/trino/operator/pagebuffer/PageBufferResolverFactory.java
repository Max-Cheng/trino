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
package io.trino.operator.pagebuffer;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.http.client.HttpClient;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.FeaturesConfig;
import io.trino.FeaturesConfig.DataIntegrityVerification;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.TaskId;
import io.trino.operator.ForExchange;
import io.trino.operator.ForPageBufferCallback;
import io.trino.operator.pagebuffer.local.LocalPageBuffer;
import io.trino.operator.pagebuffer.remote.RemotePageBuffer;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;

public class PageBufferResolverFactory
{
    private final String selfAddress;
    private final HttpClient httpClient;
    private final DataIntegrityVerification dataIntegrityVerification;
    private final Provider<SqlTaskManager> taskManagerProvider;
    private final ScheduledExecutorService scheduledExecutor;
    private final ExecutorService pageBufferClientCallbackExecutor;

    @Inject
    public PageBufferResolverFactory(
            NodeInfo nodeInfo,
            @ForExchange HttpClient httpClient,
            FeaturesConfig featuresConfig,
            Provider<SqlTaskManager> taskManagerProvider,
            @ForExchange ScheduledExecutorService scheduledExecutor,
            @ForPageBufferCallback ExecutorService pageBufferClientCallbackExecutor)
    {
        this.selfAddress = requireNonNull(nodeInfo, "nodeInfo is null").getExternalAddress();
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.dataIntegrityVerification = requireNonNull(featuresConfig, "featuresConfig is null").getExchangeDataIntegrityVerification();
        this.taskManagerProvider = requireNonNull(taskManagerProvider, "taskManagerProvider is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.pageBufferClientCallbackExecutor = requireNonNull(pageBufferClientCallbackExecutor, "pageBufferClientCallbackExecutor is null");
    }

    public PageBufferResolver createRemotePageBuffer(
            DataSize maxResponseSize,
            Duration maxErrorDuration,
            boolean acknowledgePages,
            TaskId taskId,
            URI location,
            PageBufferCallBack callback)
    {
        return new RemotePageBuffer(
                selfAddress,
                httpClient,
                dataIntegrityVerification,
                maxResponseSize,
                maxErrorDuration,
                acknowledgePages,
                taskId,
                location,
                callback,
                scheduledExecutor,
                pageBufferClientCallbackExecutor);
    }

    public PageBufferResolver createLocalPageBuffer(
            DataSize maxResponseSize,
            Duration maxErrorDuration,
            TaskId taskId,
            URI location,
            PageBufferCallBack callback)
    {
        return new LocalPageBuffer(
                taskManagerProvider.get(),
                maxResponseSize,
                maxErrorDuration,
                taskId,
                location,
                callback,
                scheduledExecutor,
                pageBufferClientCallbackExecutor);
    }
}
