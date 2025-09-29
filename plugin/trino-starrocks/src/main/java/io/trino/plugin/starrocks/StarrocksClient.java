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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.mysql.cj.jdbc.Driver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.EmptyCredentialProvider;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.connector.informationschema.InformationSchemaTable.INFORMATION_SCHEMA;
import static io.trino.plugin.starrocks.StarrocksUtils.genSQL;
import static io.trino.plugin.starrocks.StarrocksUtils.getURLForQueryPlan;
import static io.trino.plugin.starrocks.StarrocksUtils.getURLForTransactionCommit;
import static io.trino.plugin.starrocks.StarrocksUtils.getURLForTransactionUUID;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static okhttp3.Credentials.basic;

public class StarrocksClient
{
    private final StarrocksConfig config;
    private final DriverConnectionFactory dbClient;

    static final Logger LOG = LoggerFactory.getLogger(StarrocksClient.class);

    static final Properties BASE_PROPERTIES = createBaseProperties();
    static final Properties LDAP_PROPERTIES = createLDAPProperties();

    @Inject
    public StarrocksClient(StarrocksConfig config, OpenTelemetry openTelemetry)
    {
        this.config = requireNonNull(config, "config is null");
        this.dbClient = createDatabaseClient(config, openTelemetry);
    }

    private DriverConnectionFactory createDatabaseClient(StarrocksConfig config, OpenTelemetry openTelemetry)
    {
        CredentialProvider authenticator = config.getPassword().isPresent()
                ? new StaticCredentialProvider(Optional.of(config.getUsername()), config.getPassword())
                : new EmptyCredentialProvider();
        try {
            if (config.isEnableLDAP()) {
                return DriverConnectionFactory.builder(new Driver(), config.getJdbcURL(), authenticator)
                        .setConnectionProperties(LDAP_PROPERTIES)
                        .setOpenTelemetry(openTelemetry)
                        .build();
            }
            return DriverConnectionFactory.builder(new Driver(), config.getJdbcURL(), authenticator)
                    .setConnectionProperties(BASE_PROPERTIES)
                    .setOpenTelemetry(openTelemetry)
                    .build();
        }
        catch (SQLException e) {
            LOG.error("Failed to create database client", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create database client", e);
        }
    }

    private static Properties createBaseProperties()
    {
        Properties properties = new Properties();
        properties.put("useSSL", "false");
        return properties;
    }

    private static Properties createLDAPProperties()
    {
        Properties props = new Properties();
        props.putAll(BASE_PROPERTIES);
        props.put("authenticationPlugins", "io.trino.plugin.starrocks.MysqlClearPasswordPluginWithoutSSL");
        props.put("defaultAuthenticationPlugin", "io.trino.plugin.starrocks.MysqlClearPasswordPluginWithoutSSL");
        props.put("disabledAuthenticationPlugins", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");
        return props;
    }

    private StarrocksQueryPlan getQueryPlan(
            String querySQL,
            String httpNode,
            StarrocksConfig config,
            StarrocksTableHandle tableHandle)
            throws IOException
    {
        String url = getURLForQueryPlan(tableHandle, httpNode).toString();
        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("sql", querySQL);
        String body = new JSONObject(bodyMap).toString();
        int requsetCode = 0;
        String respString = "";
        for (int i = 0; i < config.getScanMaxRetries(); i++) {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type", "application/json;charset=UTF-8");
                if (config.getPassword().orElse(null) != null) {
                    post.setHeader("Authorization", basic(config.getUsername(), config.getPassword().orElse(null)));
                }
                post.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
                try (CloseableHttpResponse response = httpClient.execute(post)) {
                    requsetCode = response.getStatusLine().getStatusCode();
                    HttpEntity respEntity = response.getEntity();
                    respString = EntityUtils.toString(respEntity, "UTF-8");
                }
            }
            if (HttpURLConnection.HTTP_OK == requsetCode || i == config.getScanMaxRetries() - 1) {
                break;
            }
            LOG.warn("Request of get query plan failed with code:{}", requsetCode);
            try {
                Thread.sleep(1000L * (i + 1));
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unable to get query plan, interrupted while doing another attempt", ex);
            }
        }
        if (200 != requsetCode) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Request of get query plan failed with code " + requsetCode + " " + respString);
        }
        if (respString.isEmpty()) {
            LOG.warn("Request failed with empty response.");
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Request failed with empty response." + requsetCode);
        }
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            JsonNode rootNode = objectMapper.readTree(respString);
            if (rootNode.has("data")) {
                respString = rootNode.get("data").toString();
            }
            return objectMapper.readValue(respString, StarrocksQueryPlan.class);
        }
        catch (IOException e) {
            LOG.error("Parse response failed", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Parse response failed", e);
        }
    }

    private static Map<String, Set<Long>> transferQueryPlanToBeXTablet(StarrocksQueryPlan queryPlan)
    {
        Map<String, Set<Long>> beXTablets = new HashMap<>();
        queryPlan.getPartitions().forEach((tabletId, routingList) -> {
            int tabletCount = Integer.MAX_VALUE;
            String candidateBe = "";
            for (String beNode : routingList.getRoutings()) {
                if (!beXTablets.containsKey(beNode)) {
                    beXTablets.put(beNode, new HashSet<>());
                    candidateBe = beNode;
                    break;
                }
                if (beXTablets.get(beNode).size() < tabletCount) {
                    candidateBe = beNode;
                    tabletCount = beXTablets.get(beNode).size();
                }
            }
            beXTablets.get(candidateBe).add(Long.valueOf(tabletId));
        });
        return beXTablets;
    }

    public List<String> getSchemaNames(ConnectorSession session)
    {
        String sql = "SHOW DATABASES";
        try (Connection connection = dbClient.openConnection(session);
                Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            List<String> databaseNames = new ArrayList<>();
            while (resultSet.next()) {
                String databaseName = resultSet.getString(1).toLowerCase(Locale.ROOT);
                databaseNames.add(databaseName);
            }
            return databaseNames;
        }
        catch (Exception e) {
            LOG.error("Execute sql {} fail", sql, e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Execute sql {} fail", sql), e);
        }
    }

    public List<SchemaTableName> listTables(Optional<String> schemaName, ConnectorSession session)
    {
        try (Connection connection = dbClient.openConnection(session);
                Statement statement = connection.createStatement()) {
            String sql = schemaName
                    .map(schema -> "SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_TYPE = 'BASE TABLE'")
                    .orElse("SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'");
            ResultSet resultSet = statement.executeQuery(sql);
            ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
            while (resultSet.next()) {
                tableNames.add(new SchemaTableName(
                        resultSet.getString(1),
                        resultSet.getString(2)));
            }
            listMaterializedViews(schemaName, session).forEach(tableNames::add);
            return tableNames.build();
        }
        catch (Exception e) {
            LOG.error("Execute sql fail", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Execute sql fail", e);
        }
    }

    public List<SchemaTableName> listMaterializedViews(Optional<String> schemaName, ConnectorSession session)
    {
        try (Connection connection = dbClient.openConnection(session);
                Statement statement = connection.createStatement()) {
            String sql = schemaName
                    .map(schema -> "SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.MATERIALIZED_VIEWS WHERE TABLE_SCHEMA = '" + schema + "' AND refresh_type != 'ROLLUP'")
                    .orElse("SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.MATERIALIZED_VIEWS WHERE refresh_type != 'ROLLUP'");
            ResultSet resultSet = statement.executeQuery(sql);
            ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
            while (resultSet.next()) {
                tableNames.add(new SchemaTableName(
                        resultSet.getString(1),
                        resultSet.getString(2)));
            }
            return tableNames.build();
        }
        catch (Exception e) {
            LOG.error("Execute sql fail", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Execute sql fail", e);
        }
    }

    public StarrocksTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        try (Connection connection = dbClient.openConnection(session);
                Statement statement = connection.createStatement()) {
            if (tableName == null) {
                throw new TableNotFoundException(tableName);
            }
            String sql = "select TABLES.TABLE_COMMENT FROM information_schema.TABLES where " +
                    "TABLES.TABLE_SCHEMA = '" + tableName.getSchemaName() + "' AND " +
                    "TABLES.TABLE_NAME = '" + tableName.getTableName() + "'";
            ResultSet resultSet = statement.executeQuery(sql);
            String tableComment = null;
            if (!resultSet.isBeforeFirst()) {
                throw new TableNotFoundException(tableName);
            }
            else {
                resultSet.next();
                tableComment = resultSet.getString("TABLE_COMMENT");
            }
            // star to build partition info
            sql = "select PARTITION_KEY from" +
                    " information_schema.tables_config where " +
                    "tables_config.TABLE_SCHEMA = '" + tableName.getSchemaName() + "' " +
                    "AND tables_config.TABLE_NAME = '" + tableName.getTableName() + "'";
            resultSet = statement.executeQuery(sql);
            String partitionKeysStr = "";
            while (resultSet.next()) {
                partitionKeysStr = resultSet.getString("PARTITION_KEY").replaceAll("`", "");
            }
            List<String> partitionKeys = List.of(partitionKeysStr.split(", "));

            Map<String, Object> properties = new HashMap<>();
            properties.put("partitioned_by", partitionKeys);

            return new StarrocksTableHandle(
                    tableName,
                    getColumnHandlers(session, tableName, partitionKeys),
                    TupleDomain.all(),
                    Optional.ofNullable(tableComment),
                    Optional.of(partitionKeys),
                    Optional.of(properties));
        }
        catch (SQLException e) {
            LOG.error("Execute sql fail", e);
        }
        return new StarrocksTableHandle(
                tableName,
                getColumnHandlers(session, tableName, Collections.emptyList()),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public List<StarrocksColumnHandle> getColumnHandlers(ConnectorSession session, SchemaTableName tableName, List<String> patitionKeys)
    {
        try (Connection connection = dbClient.openConnection(session);
                Statement statement = connection.createStatement()) {
            String sql = "SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, EXTRA, COLUMN_COMMENT,COLUMN_SIZE,DECIMAL_DIGITS " +
                    "FROM " + INFORMATION_SCHEMA + ".columns " +
                    "WHERE TABLE_SCHEMA = '" + tableName.getSchemaName() +
                    "' AND TABLE_NAME = '" + tableName.getTableName() + "'";
            ResultSet resultSet = statement.executeQuery(sql);
            ImmutableList.Builder<StarrocksColumnHandle> columnMetadata = ImmutableList.builder();
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                int ordinalPosition = resultSet.getInt("ORDINAL_POSITION");
                String dataType = resultSet.getString("DATA_TYPE");
                String columnType = resultSet.getString("COLUMN_TYPE");
                boolean isNullable = resultSet.getBoolean("IS_NULLABLE");
                String extra = patitionKeys.contains(columnName) ? "partition key" : resultSet.getString("EXTRA");
                String comment = resultSet.getString("COLUMN_COMMENT");
                int columnSize = resultSet.getInt("COLUMN_SIZE");
                int decimalDigits = resultSet.getInt("DECIMAL_DIGITS");
                columnMetadata.add(new StarrocksColumnHandle(
                        columnName,
                        ordinalPosition,
                        dataType,
                        columnType,
                        isNullable,
                        extra,
                        comment,
                        columnSize,
                        decimalDigits));
            }
            return columnMetadata.build();
        }
        catch (Exception e) {
            LOG.error("Execute sql {} fail", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public StarrocksQueryInfo getQueryInfo(StarrocksTableHandle tableHandle, TupleDomain<ColumnHandle> predicate, int domainLimit)
            throws IOException
    {
        String sql = genSQL(
                Optional.of(
                        tableHandle.getColumns().stream().map(StarrocksColumnHandle::getName).collect(toImmutableList())),
                tableHandle.getSchemaTableName().getSchemaName(),
                tableHandle.getSchemaTableName().getTableName(),
                predicate,
                domainLimit);
        LOG.info("Generated SQL: {}", sql);
        StarrocksQueryPlan plan = getQueryPlan(
                sql,
                config.getScanURL(),
                config,
                tableHandle);
        Map<String, Set<Long>> beXTablets = transferQueryPlanToBeXTablet(plan);
        List<StarrocksQueryBeXTablets> queryBeXTabletsList = new ArrayList<>();
        beXTablets.forEach((key, value) -> {
            StarrocksQueryBeXTablets queryBeXTablets = new StarrocksQueryBeXTablets(key, new ArrayList<>(value));
            queryBeXTabletsList.add(queryBeXTablets);
        });
        return new StarrocksQueryInfo(plan, queryBeXTabletsList);
    }

    public List<StarrocksSplit> buildStarrocksSplits(StarrocksTableHandle tableHandle, TupleDomain<ColumnHandle> predicate, int domainLimit)
    {
        List<StarrocksSplit> splits = new ArrayList<>();
        try {
            StarrocksQueryInfo queryInfo = getQueryInfo(tableHandle, predicate, domainLimit);
            Map<String, Set<Long>> beXTablets = transferQueryPlanToBeXTablet(queryInfo.getQueryPlan());
            String schemaName = tableHandle.getSchemaTableName().getSchemaName();
            String tableName = tableHandle.getSchemaTableName().getTableName();
            String opaquedQueryPlan = queryInfo.getQueryPlan().getOpaqued_query_plan();
            for (Map.Entry<String, Set<Long>> entry : beXTablets.entrySet()) {
                String beAddress = entry.getKey();
                for (Long tabletId : entry.getValue()) {
                    splits.add(new StarrocksSplit(
                            schemaName,
                            tableName,
                            ImmutableList.of(tabletId),
                            beAddress,
                            opaquedQueryPlan));
                }
            }
        }
        catch (IOException e) {
            LOG.error("Get query info fail", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Get query info fail", e);
        }
        return splits;
    }

    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table)
    {
        StarrocksTableHandle tableHandle = (StarrocksTableHandle) table;
        if (tableHandle == null) {
            return TableStatistics.empty();
        }

        try (Connection connection = dbClient.openConnection(session);
                Statement statement = connection.createStatement()) {
            String sql = "SELECT COLUMN_NAME, ROW_COUNT, DATA_SIZE, NULL_COUNT " +
                    "FROM _statistics_.column_statistics " +
                    "WHERE TABLE_NAME = " + "'" + tableHandle.getSchemaTableName().getSchemaName() + "."
                    + tableHandle.getSchemaTableName().getTableName() + "'";
            ResultSet resultSet = statement.executeQuery(sql);
            TableStatistics.Builder builder = new TableStatistics.Builder();
            double rowNum = 0;
            while (resultSet.next()) {
                rowNum += 1;
                String columnName = resultSet.getString(1);
                double rowCount = resultSet.getDouble(2);
                double dataSize = resultSet.getDouble(3);
                double nullCount = resultSet.getDouble(4);
                ColumnStatistics.Builder columnStatistics = new ColumnStatistics.Builder();
                columnStatistics.setNullsFraction(Estimate.of(nullCount / rowCount));
                columnStatistics.setDataSize(Estimate.of(dataSize));
                tableHandle.getColumns().stream().filter(c -> c.getName().equals(columnName)).findFirst().ifPresent(
                        handler -> {
                            builder.setColumnStatistics(handler, columnStatistics.build());
                        });
            }
            builder.setRowCount(Estimate.of(rowNum));
            return builder.build();
        }
        catch (Exception e) {
            LOG.error("Execute sql fail", e);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Execute sql fail", e);
        }
    }

    public String createInsertTransaction(ConnectorSession session, StarrocksTableHandle starrocksTableHandle, String host)
            throws Exception
    {
        String queryID = session.getQueryId();
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(getURLForTransactionUUID(host).toString());
            post.setHeader("Expect", "100-continue");
            post.setHeader("Authorization", basic(config.getUsername(), config.getPassword().orElse("")));
            post.setHeader("db", starrocksTableHandle.getSchemaTableName().getSchemaName());
            post.setHeader("table", starrocksTableHandle.getSchemaTableName().getTableName());
            post.setHeader("format", "JSON");
            post.setHeader("label", "insert" + queryID);
            try (CloseableHttpResponse response = httpClient.execute(post)) {
                int requsetCode = response.getStatusLine().getStatusCode();
                HttpEntity respEntity = response.getEntity();
                String respString = EntityUtils.toString(respEntity, "UTF-8");
                if (HttpURLConnection.HTTP_OK == requsetCode) {
                    JSONObject jsonObject = new JSONObject(respString);
                    String status = jsonObject.getString("Status");
                    if (status.equals("OK")) {
                        return jsonObject.getString("Label");
                    }
                    else {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create insert transaction" + respString);
                    }
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create insert transaction");
                }
            }
        }
    }

    public JSONObject commitInsertTransaction(ConnectorSession session, StarrocksInsertTableHandle tableHandle, String uuid, String host)
            throws Exception
    {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(getURLForTransactionCommit(host).toString());
            post.setHeader("Expect", "100-continue");
            post.setHeader("db", tableHandle.getSchemaTableName().getSchemaName());
            post.setHeader("format", "JSON");
            post.setHeader("table", tableHandle.getSchemaTableName().getTableName());
            post.setHeader("Authorization", basic(config.getUsername(), config.getPassword().orElse("")));
            post.setHeader("Label", uuid);
            try (CloseableHttpResponse response = httpClient.execute(post)) {
                int requsetCode = response.getStatusLine().getStatusCode();
                HttpEntity respEntity = response.getEntity();
                String respString = EntityUtils.toString(respEntity, "UTF-8");
                if (HttpURLConnection.HTTP_OK == requsetCode) {
                    JSONObject jsonObject = new JSONObject(respString);
                    String status = jsonObject.getString("Status");
                    if (status.equals("OK") && jsonObject.getString("Message").isEmpty()) {
                        LOG.info("Commit insert transaction success");
                        return jsonObject;
                    }
                    else {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to commit insert transaction");
                    }
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, respString);
                }
            }
        }
    }

    public StarrocksConfig getConfig()
    {
        return config;
    }
}
