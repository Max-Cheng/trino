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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class StarrocksMetadata
        implements ConnectorMetadata
{
    private final StarrocksConfig config;
    private final StarrocksClient client;

    StarrocksMetadata(StarrocksClient client, StarrocksConfig config)
    {
        this.config = config;
        this.client = client;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return listSchemaNames(session).contains(schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return client.getClient().getSchemaNames(session);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return client.getClient().listTables(schemaName, session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        return client.getClient().getTableHandle(session, tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        StarrocksTableHandle tableHandle = (StarrocksTableHandle) table;
        return new ConnectorTableMetadata(
                tableHandle.getSchemaTableName(),
                tableHandle.getColumns().stream()
                        .map(column -> {
                            ColumnMetadata.Builder builder = ColumnMetadata.builder();
                            builder.setName(column.getName());
                            builder.setType(StarrocksTypeMapper.toTrinoType(
                                    column.getType(),
                                    column.getColumnType(),
                                    column.getColumnSize(),
                                    column.getDecimalDigits()));
                            builder.setNullable(column.isNullable());
                            builder.setComment(Optional.of(column.getComment()));
                            builder.setExtraInfo(Optional.of(column.getExtra()));
                            return builder.build();
                        })
                        .collect(toImmutableList()),
                tableHandle.getProperties().orElse(ImmutableMap.of()),
                tableHandle.getComment());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        ((StarrocksTableHandle) tableHandle).getColumns().forEach(column -> columnHandles.put(column.getName(), column));
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((StarrocksTableHandle) tableHandle).getColumns().stream()
                .filter(column -> column.getName().equals(((StarrocksColumnHandle) columnHandle).getName()))
                .findFirst()
                .map(column -> new ColumnMetadata(
                        column.getName(),
                        StarrocksTypeMapper.toTrinoType(column.getType(), column.getColumnType(), column.getColumnSize(), column.getDecimalDigits())))
                .orElseThrow(() -> new IllegalArgumentException("Column not found: " + columnHandle));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        StarrocksTableHandle starrocksHandle = (StarrocksTableHandle) handle;
        Set<StarrocksColumnHandle> currentColumns = new LinkedHashSet<>(starrocksHandle.getColumns());

        ImmutableMap<String, StarrocksColumnHandle> projectedColumns = assignments.entrySet().stream()
                .filter(entry -> entry.getValue() instanceof StarrocksColumnHandle)
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> (StarrocksColumnHandle) entry.getValue()));

        // 检查是否需要应用新的投影
        if (projectedColumns.size() == currentColumns.size() &&
                new LinkedHashSet<>(projectedColumns.values()).equals(currentColumns)) {
            return Optional.empty();
        }

        // 创建新的 TableHandle，只包含投影的列
        StarrocksTableHandle newTableHandle = new StarrocksTableHandle(
                starrocksHandle.getSchemaTableName(),
                ImmutableList.copyOf(projectedColumns.values()),
                starrocksHandle.getConstraint(),
                starrocksHandle.getComment(),
                starrocksHandle.getPartitionKey(),
                starrocksHandle.getProperties());

        List<Assignment> assignmentList = projectedColumns.entrySet().stream()
                .map(entry ->
                        new Assignment(
                                entry.getKey(),
                                entry.getValue(),
                                StarrocksTypeMapper.toTrinoType(entry.getValue().getType(), entry.getValue().getColumnType(), entry.getValue().getColumnSize(), entry.getValue().getDecimalDigits())))
                .collect(toImmutableList());

        boolean allExpressionsHandled = projections.stream()
                .allMatch(expression -> isHandledExpression(expression, projectedColumns));

        return Optional.of(new ProjectionApplicationResult<>(
                newTableHandle,
                projections,
                assignmentList,
                allExpressionsHandled));
    }

    private boolean isHandledExpression(ConnectorExpression expression, Map<String, StarrocksColumnHandle> projectedColumns)
    {
        if (isVariableReferenceExpression(expression)) {
            return projectedColumns.containsKey(getVariableName(expression));
        }
        return false;
    }

    private boolean isVariableReferenceExpression(ConnectorExpression expression)
    {
        return expression instanceof Variable;
    }

    private String getVariableName(ConnectorExpression expression)
    {
        if (expression instanceof Variable) {
            return ((Variable) expression).getName();
        }
        throw new IllegalArgumentException("Expression is not a Variable");
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        StarrocksTableHandle handle = (StarrocksTableHandle) table;
        TupleDomain<ColumnHandle> constraintSummary = constraint.getSummary();

        if (constraintSummary.isNone()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();

        if (oldDomain.contains(constraintSummary) && !oldDomain.isAll()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraintSummary);

        StarrocksTableHandle newHandle = new StarrocksTableHandle(
                handle.getSchemaTableName(),
                handle.getColumns(),
                newDomain,
                handle.getComment(),
                handle.getPartitionKey(),
                handle.getProperties());

        return Optional.of(new ConstraintApplicationResult<>(newHandle, constraint.getSummary(), constraint.getExpression(), false));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return this.client.getClient().getTableStatistics(session, tableHandle);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        StarrocksTableHandle starrocksTableHandle = (StarrocksTableHandle) tableHandle;
        String uuid;
        Random rand = new Random();
        String host = config.getLoadURL().get(rand.nextInt(config.getLoadURL().size()));
        try {
            uuid = this.client.getClient().createInsertTransaction(
                    session,
                    starrocksTableHandle,
                    host);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new StarrocksInsertTableHandle(
                starrocksTableHandle.getSchemaTableName(),
                columns.stream().map(column -> (StarrocksColumnHandle) column).collect(toImmutableList()),
                uuid,
                host);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        StarrocksInsertTableHandle starrocksInsertTableHandle = (StarrocksInsertTableHandle) insertHandle;
        try {
            this.client.getClient().commitInsertTransaction(
                    session,
                    starrocksInsertTableHandle,
                    starrocksInsertTableHandle.getUuid(),
                    starrocksInsertTableHandle.getHost());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return Optional.empty();
    }

    @Override
    public boolean supportsMissingColumnsOnInsert()
    {
        return true;
    }
}
