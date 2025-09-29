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

import com.starrocks.shade.org.apache.thrift.protocol.TBinaryProtocol;
import com.starrocks.shade.org.apache.thrift.protocol.TProtocol;
import com.starrocks.shade.org.apache.thrift.transport.TSocket;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.thrift.TScanCloseParams;
import com.starrocks.thrift.TScanNextBatchParams;
import com.starrocks.thrift.TScanOpenParams;
import com.starrocks.thrift.TScanOpenResult;
import com.starrocks.thrift.TStarrocksExternalService;
import com.starrocks.thrift.TStatusCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class StarrocksBeReader
        implements AutoCloseable
{
    private final String host;
    private final StarrocksConfig config;
    private final SchemaTableName schemaTableName;
    private String contextId;
    private int readerOffset;
    private final TStarrocksExternalService.Client client;

    StarrocksBeReader(
            StarrocksConfig config,
            String beInfo,
            List<ColumnHandle> columns,
            SchemaTableName schemaTableName)
    {
        String[] beNode = beInfo.split(":");
        String ip = beNode[0].trim();
        int port = Integer.parseInt(beNode[1].trim());

        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        TSocket socket = new TSocket(ip, port);
        try {
            socket.open();
        }
        catch (Exception e) {
            socket.close();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to open socket to " + ip + ":" + port + e.getMessage());
        }
        TProtocol protocol = factory.getProtocol(socket);
        this.host = ip;
        this.client = new TStarrocksExternalService.Client(protocol);
        this.config = config;
        this.schemaTableName = schemaTableName;
    }

    public TStarrocksExternalService.Client getClient()
    {
        return client;
    }

    public void openScanner(
            List<Long> tablets,
            String opaquedQueryPlan)
    {
        TScanOpenParams params = getTScanOpenParams(tablets, opaquedQueryPlan);
        TScanOpenResult result = null;
        try {
            result = client.open_scanner(params);
            if (!result.getStatus().getStatus_code().equals(TStatusCode.OK)) {
                this.close();
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        "Failed to open scanner "
                                + result.getStatus().getStatus_code()
                                + result.getStatus().getError_msgs());
            }
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to open scanner " + e.getMessage());
        }
        this.contextId = result.getContext_id();
    }

    private TScanOpenParams getTScanOpenParams(List<Long> tablets, String opaquedQueryPlan)
    {
        TScanOpenParams params = new TScanOpenParams();

        params.setTablet_ids(tablets);
        params.setCluster("default_cluster");
        params.setOpaqued_query_plan(opaquedQueryPlan);
        params.setDatabase(schemaTableName.getSchemaName());
        params.setTable(schemaTableName.getTableName());
        params.setUser(config.getUsername());
        params.setPasswd(config.getPassword().orElse(null));
        // TODO:this param should be configurable
        params.setBatch_size(4096);
        short keepAliveMin = (short) Math.min(Short.MAX_VALUE, 10);
        params.setKeep_alive_min(keepAliveMin);
        params.setQuery_timeout(600);
        params.setMem_limit(1024 * 1024 * 1024L);
        return params;
    }

    public TScanBatchResult getNextBatch()
    {
        TScanNextBatchParams params = new TScanNextBatchParams();
        params.setContext_id(this.contextId);
        params.setOffset(this.readerOffset);
        TScanBatchResult result;
        try {
            result = client.get_next(params);
            if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        "Failed to get next from be -> ip:[" + host + "] "
                                + result.getStatus().getStatus_code() + " msg:" + result.getStatus().getError_msgs());
            }
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e.getMessage());
        }
        return result;
    }

    public int getReaderOffset()
    {
        return this.readerOffset;
    }

    public void setReaderOffset(int readerOffset)
    {
        this.readerOffset = readerOffset;
    }

    @Override
    public void close()
    {
        if (contextId == null) {
            return;
        }
        TScanCloseParams tScanCloseParams = new TScanCloseParams();
        tScanCloseParams.setContext_id(this.contextId);
        try {
            this.client.close_scanner(tScanCloseParams);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e.getMessage());
        }
    }
}
