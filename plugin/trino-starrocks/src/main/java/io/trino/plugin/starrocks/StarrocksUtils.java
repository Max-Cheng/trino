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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;

public class StarrocksUtils
{
    private StarrocksUtils() {}

    public static URI getURLForQueryPlan(StarrocksTableHandle tableHandle, String scanUrl)
    {
        return URI.create(java.lang.String.format("http://%s/api/%s/%s/_query_plan",
                scanUrl,
                tableHandle.getSchemaTableName().getSchemaName(),
                tableHandle.getSchemaTableName().getTableName()));
    }

    public static URI getURLForTransactionUUID(String host)
    {
        return URI.create(java.lang.String.format("http://%s/api/transaction/begin", host));
    }

    public static URI getURLForTransactionCommit(String host)
    {
        return URI.create(java.lang.String.format("http://%s/api/transaction/commit", host));
    }

    public static String formatLiteral(Object value, Type type)
    {
        value = type.getObjectValue(nativeValueToBlock(type, value), 0);
        if (value == null) {
            return "NULL";
        }
        if (type instanceof VarcharType) {
            return "'" + value.toString().replace("'", "''") + "'";
        }
        else if (type instanceof BigintType || type instanceof IntegerType || type instanceof SmallintType || type instanceof TinyintType) {
            return value.toString();
        }
        else if (type instanceof DoubleType || type instanceof RealType || type instanceof DecimalType) {
            return new BigDecimal(value.toString()).toPlainString();
        }
        else if (type instanceof BooleanType) {
            return ((Boolean) value) ? "1" : "0";
        }
        else if (type instanceof DateType) {
            return "'" + value + "'";
        }
        else if (type instanceof TimestampType) {
            return "'" + value + "'";
        }

        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported literal type: " + type.getDisplayName());
    }

    public static String buildPredicate(TupleDomain<ColumnHandle> constraint, int domainLimit)
    {
        if (constraint.isNone()) {
            return "1 = 0";
        }

        if (constraint.isAll()) {
            return null;
        }

        List<String> columnPredicates = new ArrayList<>();

        for (Map.Entry<ColumnHandle, Domain> entry : constraint.getDomains().get().entrySet()) {
            StarrocksColumnHandle columnHandle = (StarrocksColumnHandle) entry.getKey();
            Domain domain = entry.getValue().simplify(domainLimit);
            String columnName = columnHandle.getName();

            Optional<String> predicate = buildColumnPredicate(columnName, domain);
            predicate.ifPresent(columnPredicates::add);
        }

        return columnPredicates.isEmpty() ? null : java.lang.String.join(" AND ", columnPredicates);
    }

    public static Optional<String> buildColumnPredicate(String columnName, Domain domain)
    {
        if (domain.isOnlyNull()) {
            return Optional.of(java.lang.String.format("`%s` IS NULL", columnName));
        }

        Optional<String> valuesPredicate = buildValueSetPredicate(columnName, domain.getValues(), domain.getType());

        if (domain.isNullAllowed()) {
            if (valuesPredicate.isPresent()) {
                return Optional.of(java.lang.String.format("(`%s` IS NULL OR %s)", columnName, valuesPredicate.get()));
            }
            else {
                return Optional.of(java.lang.String.format("`%s` IS NULL", columnName));
            }
        }

        return valuesPredicate;
    }

    public static Optional<String> buildValueSetPredicate(String columnName, ValueSet valueSet, Type type)
    {
        if (valueSet.isNone()) {
            return Optional.empty();
        }

        if (valueSet.isAll()) {
            return Optional.of(java.lang.String.format("`%s` IS NOT NULL", columnName));
        }

        if (valueSet instanceof EquatableValueSet) {
            return buildEquatableValueSetPredicate(columnName, (EquatableValueSet) valueSet);
        }

        if (valueSet instanceof SortedRangeSet) {
            return buildSortedRangeSetPredicate(columnName, (SortedRangeSet) valueSet);
        }

        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported ValueSet type: " + valueSet.getClass().getSimpleName());
    }

    public static Optional<String> buildEquatableValueSetPredicate(String columnName, EquatableValueSet valueSet)
    {
        List<String> values = valueSet.getValues().stream()
                .map(value -> formatLiteral(value, valueSet.getType()))
                .collect(Collectors.toList());

        if (values.isEmpty()) {
            return Optional.empty();
        }

        if (values.size() == 1) {
            return Optional.of(java.lang.String.format("`%s` = %s", columnName, values.getFirst()));
        }

        return Optional.of(java.lang.String.format("`%s` IN (%s)", columnName, java.lang.String.join(", ", values)));
    }

    public static Optional<String> buildSortedRangeSetPredicate(String columnName, SortedRangeSet rangeSet)
    {
        List<Range> ranges = rangeSet.getOrderedRanges();

        if (ranges.isEmpty()) {
            return Optional.empty();
        }

        RangePartition partition = partitionRanges(ranges);
        List<String> conditions = new ArrayList<>();

        if (!partition.singleValues.isEmpty()) {
            conditions.add(buildSingleValuesCondition(columnName, partition.singleValues));
        }

        for (Range range : partition.intervals) {
            conditions.add(buildRangeCondition(columnName, range));
        }

        if (conditions.isEmpty()) {
            return Optional.empty();
        }

        if (conditions.size() == 1) {
            return Optional.of(conditions.getFirst());
        }

        return Optional.of("(" + java.lang.String.join(" OR ", conditions) + ")");
    }

    private static RangePartition partitionRanges(List<Range> ranges)
    {
        List<Range> singleValues = new ArrayList<>();
        List<Range> intervals = new ArrayList<>();

        for (Range range : ranges) {
            if (range.isSingleValue()) {
                singleValues.add(range);
            }
            else {
                intervals.add(range);
            }
        }

        return new RangePartition(singleValues, intervals);
    }

    public static String buildSingleValuesCondition(String columnName, List<Range> singleValueRanges)
    {
        if (singleValueRanges.size() == 1) {
            Range range = singleValueRanges.get(0);
            return String.format("`%s` = %s", columnName,
                    formatLiteral(range.getSingleValue(), range.getType()));
        }

        List<String> values = singleValueRanges.stream()
                .map(range -> formatLiteral(range.getSingleValue(), range.getType()))
                .collect(Collectors.toList());

        return String.format("`%s` IN (%s)", columnName, java.lang.String.join(", ", values));
    }

    public static String buildRangeCondition(String columnName, Range range)
    {
        List<String> conditions = new ArrayList<>();

        if (!range.isLowUnbounded()) {
            String operator = range.isLowInclusive() ? ">=" : ">";
            conditions.add(String.format("`%s` %s %s", columnName, operator,
                    formatLiteral(range.getLowBoundedValue(), range.getType())));
        }

        if (!range.isHighUnbounded()) {
            String operator = range.isHighInclusive() ? "<=" : "<";
            conditions.add(String.format("`%s` %s %s", columnName, operator,
                    formatLiteral(range.getHighBoundedValue(), range.getType())));
        }

        if (conditions.isEmpty()) {
            return String.format("`%s` IS NOT NULL", columnName);
        }

        if (conditions.size() == 1) {
            return conditions.getFirst();
        }

        return "(" + String.join(" AND ", conditions) + ")";
    }

    public static String genSQL(Optional<List<String>> columns,
            String schemaName, String tableName,
            TupleDomain<ColumnHandle> predicate,
            int domainLimit)
    {
        List<String> columnsList = ImmutableList.copyOf(columns.orElse(new ArrayList<>()));
        String columnsStr = columnsList.isEmpty() ? "1" :
                columnsList.stream().map(column -> "`" + column + "`").collect(Collectors.joining(", "));

        String sql = "SELECT " + columnsStr + " FROM " + "`" + schemaName + "`" + "." + "`" + tableName + "`";

        String whereClause = buildPredicate(predicate, domainLimit);
        if (whereClause != null && !whereClause.isBlank()) {
            sql += " WHERE " + whereClause;
        }

        return sql;
    }

    private static class RangePartition
    {
        final List<Range> singleValues;
        final List<Range> intervals;

        RangePartition(List<Range> singleValues, List<Range> intervals)
        {
            this.singleValues = singleValues;
            this.intervals = intervals;
        }
    }
}
