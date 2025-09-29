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
package io.trino.plugin.starrocks;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import okhttp3.Call;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class StarrocksPageSink
        implements ConnectorPageSink
{
    private final SchemaTableName dstTableName;
    private final List<StarrocksColumnHandle> columns;
    private final String uuid;
    private final OkHttpClient client;
    private final String credential;
    private final String host;
    private final AtomicReference<RuntimeException> failureReference = new AtomicReference<>();
    private final AtomicLong completedBytes = new AtomicLong(0);

    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    public StarrocksPageSink(
            SchemaTableName dstTableName,
            List<StarrocksColumnHandle> columns,
            ConnectorSession session,
            String uuid,
            StarrocksConfig config,
            String host)
    {
        this.dstTableName = requireNonNull(dstTableName, "dstTableName is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.uuid = requireNonNull(uuid);
        this.credential = Credentials.basic(config.getUsername(), config.getPassword().orElse(""));
        this.client = new OkHttpClient.Builder().build();
        this.host = requireNonNull(host);
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes.get();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        byte[] requestBody = starrocksStreamLoadBodyBuilder(page);
        RequestBody body = RequestBody.create(requestBody, JSON);
        Request request = starrocksStreamLoadRequest(body);
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, response.message());
            }
            JSONObject jsonObject = new JSONObject(response.body().string());
            String status = jsonObject.getString("Status");
            if (!"OK".equals(status)) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to upload data into starrocks");
            }
            completedBytes.addAndGet(body.contentLength());
        }
        catch (IOException e) {
            failureReference.set(new TrinoException(GENERIC_INTERNAL_ERROR, e));
            return CompletableFuture.failedFuture(e);
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (failureReference.get() != null) {
            return CompletableFuture.failedFuture(failureReference.get());
        }
        return CompletableFuture.completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        this.client.dispatcher().runningCalls().forEach(Call::cancel);
        try {
            rollbackInsertTransaction(uuid, host);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public void rollbackInsertTransaction(String uuid, String host)
            throws Exception
    {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(getURLForTransactionRollback(host).toString());
            post.setHeader("Expect", "100-continue");
            post.setHeader("Authorization", this.credential);
            post.setHeader("db", dstTableName.getSchemaName());
            post.setHeader("table", dstTableName.getTableName());
            post.setHeader("Label", uuid);
            try (CloseableHttpResponse response = httpClient.execute(post)) {
                int requsetCode = response.getStatusLine().getStatusCode();
                HttpEntity respEntity = response.getEntity();
                String respString = EntityUtils.toString(respEntity, "UTF-8");
                if (HttpURLConnection.HTTP_OK == requsetCode) {
                    JSONObject jsonObject = new JSONObject(respString);
                    String status = jsonObject.getString("Status");
                    if (!(status.equals("OK") && jsonObject.getString("Message").isEmpty())) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to rollback insert transaction" + respString);
                    }
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, respString);
                }
            }
        }
    }

    private byte[] starrocksStreamLoadBodyBuilder(Page page)
    {
        StringBuilder content = new StringBuilder();
        for (int position = 0; position < page.getPositionCount(); position++) {
            JSONObject jsonObject = new JSONObject();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                if (!block.isNull(position)) {
                    StarrocksColumnHandle col = columns.get(channel);
                    Type type = StarrocksTypeMapper.toTrinoType(col);
                    Object value = StarrocksValueEncoder.toJavaObject(null, type, block, position);
                    jsonObject.put(col.getName(), value);
                }
            }
            content.append(jsonObject);
            content.append("\n");
        }
        return content.toString().getBytes(StandardCharsets.UTF_8);
    }

    private Request starrocksStreamLoadRequest(RequestBody body)
    {
        Request.Builder requestBuilder = new Request.Builder()
                .addHeader("Authorization", this.credential)
                .addHeader("Expect", "100-continue")
                .addHeader("db", dstTableName.getSchemaName())
                .addHeader("table", dstTableName.getTableName())
                .addHeader("label", uuid)
                .addHeader("format", "JSON");
        return requestBuilder
                .url(getURLForTransactionUpload(host).toString())
                .put(body)
                .build();
    }

    private URI getURLForTransactionRollback(String host)
    {
        return URI.create(String.format("http://%s/api/transaction/rollback", host));
    }

    private URI getURLForTransactionUpload(String host)
    {
        return URI.create(String.format("http://%s/api/transaction/load", host));
    }
}
