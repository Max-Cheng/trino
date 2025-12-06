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
package io.trino.parquet.writer;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.BoundaryOrder;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageLocation;
import org.apache.parquet.schema.PrimitiveType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PageStatisticsCollector
{
    private final List<Statistics> pageStatistics = new ArrayList<>();
    private final List<Long> pageOffsets = new ArrayList<>();
    private final List<Integer> pageSizes = new ArrayList<>();
    private final List<Long> pageRowCounts = new ArrayList<>();
    private final PrimitiveType primitiveType;

    public PageStatisticsCollector(PrimitiveType primitiveType)
    {
        this.primitiveType = requireNonNull(primitiveType, "primitiveType is null");
    }

    public void addPageStatistics(Statistics<?> statistics, long offset, int size, long rowCount)
    {
        pageStatistics.add(statistics);
        pageOffsets.add(offset);
        pageSizes.add(size);
        pageRowCounts.add(rowCount);
    }

    public Optional<ColumnIndex> buildColumnIndex()
    {
        if (pageStatistics.isEmpty()) {
            return Optional.empty();
        }

        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<Long> nullCounts = new ArrayList<>();

        for (Statistics stats : pageStatistics) {
            if (stats.hasNonNullValue()) {
                minValues.add(ByteBuffer.wrap(stats.getMinBytes()));
                maxValues.add(ByteBuffer.wrap(stats.getMaxBytes()));
                nullPages.add(false);
            }
            else {
                minValues.add(ByteBuffer.allocate(0));
                maxValues.add(ByteBuffer.allocate(0));
                nullPages.add(true);
            }
            nullCounts.add(stats.getNumNulls());
        }

        BoundaryOrder boundaryOrder = detectBoundaryOrder(minValues, maxValues, nullPages);
        ColumnIndex columnIndex = new ColumnIndex(nullPages, minValues, maxValues, boundaryOrder);
        columnIndex.setNull_counts(nullCounts);
        return Optional.of(columnIndex);
    }

    private BoundaryOrder detectBoundaryOrder(List<ByteBuffer> minValues, List<ByteBuffer> maxValues, List<Boolean> nullPages)
    {
        int numPages = minValues.size();
        if (numPages <= 1) {
            return BoundaryOrder.UNORDERED;
        }

        // Conservative strategy: Only detect order for small row groups (<=30 pages)
        // For large row groups, sampling might miss ordering violations in the middle,
        // which could lead to incorrect query results if marked as ordered.
        // This trades potential optimization for correctness guarantees.
        if (numPages > 30) {
            return BoundaryOrder.UNORDERED;
        }

        // Small row group: Check all pages
        // Matches Apache Parquet's logic: ascending requires min[i] <= min[i+1] && max[i] <= max[i+1]
        boolean ascending = true;
        boolean descending = true;
        ByteBuffer prevMin = null;
        ByteBuffer prevMax = null;

        for (int i = 0; i < numPages; i++) {
            if (nullPages.get(i)) {
                continue;
            }

            ByteBuffer currentMin = minValues.get(i);
            ByteBuffer currentMax = maxValues.get(i);

            if (prevMin != null && prevMax != null) {
                // Ascending: min[i-1] <= min[i] && max[i-1] <= max[i]
                int minComparison = prevMin.compareTo(currentMin);
                int maxComparison = prevMax.compareTo(currentMax);

                if (minComparison > 0 || maxComparison > 0) {
                    ascending = false;
                }

                // Descending: min[i-1] >= min[i] && max[i-1] >= max[i]
                if (minComparison < 0 || maxComparison < 0) {
                    descending = false;
                }

                // Early exit if we know it's unordered
                if (!ascending && !descending) {
                    return BoundaryOrder.UNORDERED;
                }
            }

            prevMin = currentMin;
            prevMax = currentMax;
        }

        if (ascending) {
            return BoundaryOrder.ASCENDING;
        }
        return BoundaryOrder.DESCENDING;
    }

    public Optional<OffsetIndex> buildOffsetIndex()
    {
        if (pageOffsets.isEmpty()) {
            return Optional.empty();
        }

        List<PageLocation> pageLocations = new ArrayList<>();
        long firstRowIndex = 0;

        for (int i = 0; i < pageOffsets.size(); i++) {
            pageLocations.add(new PageLocation(
                    pageOffsets.get(i),
                    pageSizes.get(i),
                    firstRowIndex));
            firstRowIndex += pageRowCounts.get(i);
        }

        return Optional.of(new OffsetIndex(pageLocations));
    }
}
