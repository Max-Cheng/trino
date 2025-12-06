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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.Column;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetWriteValidation;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.parquet.writer.ColumnWriter.BufferData;
import io.trino.spi.Page;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.format.BloomFilterAlgorithm;
import org.apache.parquet.format.BloomFilterCompression;
import org.apache.parquet.format.BloomFilterHash;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SplitBlockAlgorithm;
import org.apache.parquet.format.Uncompressed;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.XxHash;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.ParquetWriteValidation.ParquetWriteValidationBuilder;
import static io.trino.parquet.metadata.PrunedBlockMetadata.createPrunedColumnsMetadata;
import static io.trino.parquet.writer.ParquetDataOutput.createDataOutput;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;

public class ParquetWriter
        implements Closeable
{
    private static final int INSTANCE_SIZE = instanceSize(ParquetWriter.class);
    public static final List<Type> SUPPORTED_BLOOM_FILTER_TYPES = ImmutableList.of(BIGINT, DOUBLE, INTEGER, REAL, UUID, VARBINARY, VARCHAR);

    private final OutputStreamSliceOutput outputStream;
    private final ParquetWriterOptions writerOption;
    private final MessageType messageType;
    private final int chunkMaxBytes;
    private final Map<List<String>, Type> primitiveTypes;
    private final CompressionCodec compressionCodec;
    private final Optional<DateTimeZone> parquetTimeZone;
    private final FileFooter fileFooter;
    private final ImmutableList.Builder<List<Optional<BloomFilter>>> bloomFilterGroups = ImmutableList.builder();
    private final ImmutableList.Builder<List<Optional<ColumnIndex>>> columnIndexGroups = ImmutableList.builder();
    private final ImmutableList.Builder<List<Optional<OffsetIndex>>> offsetIndexGroups = ImmutableList.builder();
    private final ImmutableList.Builder<RowGroupMetadata> rowGroupMetadataList = ImmutableList.builder();
    private final Optional<ParquetWriteValidationBuilder> validationBuilder;

    private List<ColumnWriter> columnWriters;
    private int rows;
    private long bufferedBytes;
    private boolean closed;
    private boolean writeHeader;
    @Nullable
    private FileMetaData fileMetaData;

    public static final Slice MAGIC = wrappedBuffer("PAR1".getBytes(US_ASCII));

    public ParquetWriter(
            OutputStream outputStream,
            MessageType messageType,
            Map<List<String>, Type> primitiveTypes,
            ParquetWriterOptions writerOption,
            CompressionCodec compressionCodec,
            String trinoVersion,
            Optional<DateTimeZone> parquetTimeZone,
            Optional<ParquetWriteValidationBuilder> validationBuilder)
    {
        this.validationBuilder = requireNonNull(validationBuilder, "validationBuilder is null");
        this.outputStream = new OutputStreamSliceOutput(requireNonNull(outputStream, "outputStream is null"));
        this.messageType = requireNonNull(messageType, "messageType is null");
        this.primitiveTypes = requireNonNull(primitiveTypes, "primitiveTypes is null");
        this.writerOption = requireNonNull(writerOption, "writerOption is null");
        this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
        this.parquetTimeZone = requireNonNull(parquetTimeZone, "parquetTimeZone is null");
        String createdBy = formatCreatedBy(requireNonNull(trinoVersion, "trinoVersion is null"));
        this.fileFooter = new FileFooter(messageType, createdBy, parquetTimeZone);

        recordValidation(validation -> validation.setTimeZone(parquetTimeZone.map(DateTimeZone::getID)));
        recordValidation(validation -> validation.setColumns(messageType.getColumns()));
        recordValidation(validation -> validation.setCreatedBy(createdBy));
        initColumnWriters();
        this.chunkMaxBytes = max(1, writerOption.getMaxRowGroupSize() / 2);
    }

    public long getWrittenBytes()
    {
        return outputStream.longSize();
    }

    public long getBufferedBytes()
    {
        return bufferedBytes;
    }

    public long getRetainedBytes()
    {
        return INSTANCE_SIZE +
                outputStream.getRetainedSize() +
                columnWriters.stream().mapToLong(ColumnWriter::getRetainedBytes).sum() +
                validationBuilder.map(ParquetWriteValidationBuilder::getRetainedSize).orElse(0L);
    }

    public void write(Page page)
            throws IOException
    {
        requireNonNull(page, "page is null");
        checkState(!closed, "writer is closed");
        if (page.getPositionCount() == 0) {
            return;
        }

        checkArgument(page.getChannelCount() == columnWriters.size());

        recordValidation(validation -> validation.addPage(page));

        int writeOffset = 0;
        while (writeOffset < page.getPositionCount()) {
            Page chunk = page.getRegion(writeOffset, min(page.getPositionCount() - writeOffset, writerOption.getBatchSize()));

            // avoid chunk with huge logical size
            while (chunk.getPositionCount() > 1 && chunk.getSizeInBytes() > chunkMaxBytes) {
                chunk = page.getRegion(writeOffset, chunk.getPositionCount() / 2);
            }

            writeOffset += chunk.getPositionCount();
            writeChunk(chunk);
        }
    }

    private void writeChunk(Page page)
            throws IOException
    {
        bufferedBytes = 0;
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            ColumnWriter writer = columnWriters.get(channel);
            writer.writeBlock(new ColumnChunk(page.getBlock(channel)));
            bufferedBytes += writer.getBufferedBytes();
        }
        rows += page.getPositionCount();

        if (bufferedBytes >= writerOption.getMaxRowGroupSize()) {
            columnWriters.forEach(ColumnWriter::close);
            flush();
            initColumnWriters();
            rows = 0;
            bufferedBytes = columnWriters.stream().mapToLong(ColumnWriter::getBufferedBytes).sum();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;

        try (outputStream) {
            columnWriters.forEach(ColumnWriter::close);
            flush();
            columnWriters = ImmutableList.of();

            // Build all lists
            List<RowGroupMetadata> rowGroupMetadata = rowGroupMetadataList.build();
            List<List<Optional<BloomFilter>>> bloomFilters = bloomFilterGroups.build();
            List<List<Optional<ColumnIndex>>> columnIndexes = columnIndexGroups.build();
            List<List<Optional<OffsetIndex>>> offsetIndexes = offsetIndexGroups.build();

            // Write indexes to file and collect their offsets
            writeBloomFilters(rowGroupMetadata, bloomFilters);
            List<List<IndexOffsets>> columnIndexOffsetsList = writeColumnIndexes(columnIndexes);
            List<List<IndexOffsets>> offsetIndexOffsetsList = writeOffsetIndexes(offsetIndexes);

            // Now create RowGroups with ColumnChunks that have index references
            for (int group = 0; group < rowGroupMetadata.size(); group++) {
                RowGroupMetadata metadata = rowGroupMetadata.get(group);
                List<org.apache.parquet.format.ColumnChunk> columnChunks = new ArrayList<>();

                for (int col = 0; col < metadata.columnMetaData.size(); col++) {
                    ColumnMetaData colMetaData = metadata.columnMetaData.get(col);
                    org.apache.parquet.format.ColumnChunk columnChunk = new org.apache.parquet.format.ColumnChunk(0);
                    columnChunk.setMeta_data(colMetaData);

                    // Set column index offsets if available
                    if (group < columnIndexOffsetsList.size() && col < columnIndexOffsetsList.get(group).size()) {
                        IndexOffsets indexOffsets = columnIndexOffsetsList.get(group).get(col);
                        if (indexOffsets != null) {
                            columnChunk.setColumn_index_offset(indexOffsets.offset);
                            columnChunk.setColumn_index_length(indexOffsets.length);
                        }
                    }

                    // Set offset index offsets if available
                    if (group < offsetIndexOffsetsList.size() && col < offsetIndexOffsetsList.get(group).size()) {
                        IndexOffsets indexOffsets = offsetIndexOffsetsList.get(group).get(col);
                        if (indexOffsets != null) {
                            columnChunk.setOffset_index_offset(indexOffsets.offset);
                            columnChunk.setOffset_index_length(indexOffsets.length);
                        }
                    }

                    columnChunks.add(columnChunk);
                }

                fileFooter.addRowGroup(new RowGroup(columnChunks, metadata.totalUncompressedBytes, metadata.rows)
                        .setTotal_compressed_size(metadata.totalCompressedBytes)
                        .setFile_offset(metadata.fileOffset));
            }

            fileMetaData = fileFooter.createFileMetadata();
            writeFooter();
        }
        bufferedBytes = 0;
    }

    public void validate(ParquetDataSource input)
            throws ParquetCorruptionException
    {
        checkState(validationBuilder.isPresent(), "validation is not enabled");
        ParquetWriteValidation writeValidation = validationBuilder.get().build();
        try {
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(input, Optional.empty(), Optional.of(writeValidation), Optional.empty());
            try (ParquetReader parquetReader = createParquetReader(input, parquetMetadata, writeValidation)) {
                for (SourcePage page = parquetReader.nextPage(); page != null; page = parquetReader.nextPage()) {
                    // fully load the page
                    page.getPage();
                }
            }
        }
        catch (IOException e) {
            if (e instanceof ParquetCorruptionException pce) {
                throw pce;
            }
            throw new ParquetCorruptionException(input.getId(), "Validation failed with exception %s", e);
        }
    }

    public FileMetaData getFileMetaData()
    {
        checkState(closed, "fileMetaData is available only after writer is closed");
        return requireNonNull(fileMetaData, "fileMetaData is null");
    }

    private ParquetReader createParquetReader(ParquetDataSource input, ParquetMetadata parquetMetadata, ParquetWriteValidation writeValidation)
            throws IOException
    {
        FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
        MessageColumnIO messageColumnIO = getColumnIO(fileMetaData.getSchema(), fileMetaData.getSchema());
        ImmutableList.Builder<Column> columnFields = ImmutableList.builder();
        for (int i = 0; i < writeValidation.getTypes().size(); i++) {
            columnFields.add(new Column(
                    messageColumnIO.getName(),
                    constructField(
                            writeValidation.getTypes().get(i),
                            lookupColumnByName(messageColumnIO, writeValidation.getColumnNames().get(i)))
                            .orElseThrow()));
        }
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileMetaData.getSchema(), fileMetaData.getSchema());
        long nextStart = 0;
        ImmutableList.Builder<RowGroupInfo> rowGroupInfoBuilder = ImmutableList.builder();
        for (BlockMetadata block : parquetMetadata.getBlocks()) {
            rowGroupInfoBuilder.add(new RowGroupInfo(createPrunedColumnsMetadata(block, input.getId(), descriptorsByPath), nextStart, Optional.empty()));
            nextStart += block.rowCount();
        }
        return new ParquetReader(
                Optional.ofNullable(fileMetaData.getCreatedBy()),
                columnFields.build(),
                false,
                rowGroupInfoBuilder.build(),
                input,
                parquetTimeZone.orElseThrow(),
                newSimpleAggregatedMemoryContext(),
                ParquetReaderOptions.defaultOptions(),
                exception -> {
                    throwIfUnchecked(exception);
                    return new RuntimeException(exception);
                },
                Optional.empty(),
                Optional.of(writeValidation),
                Optional.empty());
    }

    private void recordValidation(Consumer<ParquetWriteValidationBuilder> task)
    {
        validationBuilder.ifPresent(task);
    }

    // Parquet File Layout:
    //
    // MAGIC
    // variable: Data
    // variable: Metadata
    // 4 bytes: MetadataLength
    // MAGIC
    private void flush()
            throws IOException
    {
        // write header
        if (!writeHeader) {
            createDataOutput(MAGIC).writeData(outputStream);
            writeHeader = true;
        }

        // get all data in buffer
        ImmutableList.Builder<BufferData> builder = ImmutableList.builder();
        for (ColumnWriter columnWriter : columnWriters) {
            columnWriter.getBuffer().forEach(builder::add);
        }
        List<BufferData> bufferDataList = builder.build();

        if (rows == 0) {
            // Avoid writing empty row groups as these are ignored by the reader
            verify(
                    bufferDataList.stream()
                            .flatMap(bufferData -> bufferData.getData().stream())
                            .allMatch(dataOutput -> dataOutput.size() == 0),
                    "Buffer should be empty when there are no rows");
            return;
        }

        // update stats
        long currentOffset = outputStream.longSize();
        ImmutableList.Builder<ColumnMetaData> columnMetaDataBuilder = ImmutableList.builder();
        for (BufferData bufferData : bufferDataList) {
            ColumnMetaData columnMetaData = bufferData.getMetaData();
            OptionalInt dictionaryPageSize = bufferData.getDictionaryPageSize();
            if (dictionaryPageSize.isPresent()) {
                columnMetaData.setDictionary_page_offset(currentOffset);
            }
            columnMetaData.setData_page_offset(currentOffset + dictionaryPageSize.orElse(0));
            columnMetaDataBuilder.add(columnMetaData);
            currentOffset += columnMetaData.getTotal_compressed_size();
        }
        updateRowGroups(columnMetaDataBuilder.build(), outputStream.longSize());

        // flush pages
        for (BufferData bufferData : bufferDataList) {
            bufferData.getData()
                    .forEach(data -> data.writeData(outputStream));
        }

        bloomFilterGroups.add(bufferDataList.stream().map(BufferData::getBloomFilter).collect(toImmutableList()));
        columnIndexGroups.add(bufferDataList.stream().map(BufferData::getColumnIndex).collect(toImmutableList()));
        offsetIndexGroups.add(bufferDataList.stream().map(BufferData::getOffsetIndex).collect(toImmutableList()));
    }

    private void writeFooter()
            throws IOException
    {
        checkState(closed);

        Slice footer = serializeFooter(fileMetaData);
        recordValidation(validation -> validation.setRowGroups(fileMetaData.getRow_groups()));
        createDataOutput(footer).writeData(outputStream);

        Slice footerSize = Slices.allocate(SIZE_OF_INT);
        footerSize.setInt(0, footer.length());
        createDataOutput(footerSize).writeData(outputStream);

        createDataOutput(MAGIC).writeData(outputStream);
    }

    private void writeBloomFilters(List<RowGroupMetadata> rowGroupMetadata, List<List<Optional<BloomFilter>>> rowGroupBloomFilters)
    {
        checkArgument(rowGroupMetadata.size() == rowGroupBloomFilters.size(),
                "Row groups size %s should match row group Bloom filter size %s",
                rowGroupMetadata.size(), rowGroupBloomFilters.size());

        for (int group = 0; group < rowGroupMetadata.size(); group++) {
            List<ColumnMetaData> columnMetaDataList = rowGroupMetadata.get(group).columnMetaData;
            List<Optional<BloomFilter>> bloomFilters = rowGroupBloomFilters.get(group);

            for (int i = 0; i < columnMetaDataList.size(); i++) {
                if (bloomFilters.get(i).isEmpty()) {
                    continue;
                }

                BloomFilter bloomFilter = bloomFilters.get(i).orElseThrow();
                long bloomFilterOffset = outputStream.longSize();
                try {
                    Util.writeBloomFilterHeader(
                            new BloomFilterHeader(
                                    bloomFilter.getBitsetSize(),
                                    BloomFilterAlgorithm.BLOCK(new SplitBlockAlgorithm()),
                                    BloomFilterHash.XXHASH(new XxHash()),
                                    BloomFilterCompression.UNCOMPRESSED(new Uncompressed())),
                            outputStream,
                            null,
                            null);
                    bloomFilter.writeTo(outputStream);
                    columnMetaDataList.get(i).setBloom_filter_offset(bloomFilterOffset);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private List<List<IndexOffsets>> writeColumnIndexes(List<List<Optional<ColumnIndex>>> rowGroupColumnIndexes)
    {
        List<List<IndexOffsets>> result = new ArrayList<>();

        for (int group = 0; group < rowGroupColumnIndexes.size(); group++) {
            List<Optional<ColumnIndex>> columnIndexes = rowGroupColumnIndexes.get(group);
            List<IndexOffsets> groupOffsets = new ArrayList<>();

            for (int i = 0; i < columnIndexes.size(); i++) {
                if (columnIndexes.get(i).isEmpty()) {
                    groupOffsets.add(null);
                    continue;
                }

                ColumnIndex columnIndex = columnIndexes.get(i).orElseThrow();
                long columnIndexOffset = outputStream.longSize();
                try {
                    Util.writeColumnIndex(columnIndex, outputStream, null, null);
                    long columnIndexLength = outputStream.longSize() - columnIndexOffset;
                    groupOffsets.add(new IndexOffsets(columnIndexOffset, (int) columnIndexLength));
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            result.add(groupOffsets);
        }
        return result;
    }

    private List<List<IndexOffsets>> writeOffsetIndexes(List<List<Optional<OffsetIndex>>> rowGroupOffsetIndexes)
    {
        List<List<IndexOffsets>> result = new ArrayList<>();

        for (int group = 0; group < rowGroupOffsetIndexes.size(); group++) {
            List<Optional<OffsetIndex>> offsetIndexes = rowGroupOffsetIndexes.get(group);
            List<IndexOffsets> groupOffsets = new ArrayList<>();

            for (int i = 0; i < offsetIndexes.size(); i++) {
                if (offsetIndexes.get(i).isEmpty()) {
                    groupOffsets.add(null);
                    continue;
                }

                OffsetIndex offsetIndex = offsetIndexes.get(i).orElseThrow();
                long offsetIndexOffset = outputStream.longSize();
                try {
                    Util.writeOffsetIndex(offsetIndex, outputStream, null, null);
                    long offsetIndexLength = outputStream.longSize() - offsetIndexOffset;
                    groupOffsets.add(new IndexOffsets(offsetIndexOffset, (int) offsetIndexLength));
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            result.add(groupOffsets);
        }
        return result;
    }

    private void updateRowGroups(List<ColumnMetaData> columnMetaData, long fileOffset)
    {
        long totalCompressedBytes = columnMetaData.stream().mapToLong(ColumnMetaData::getTotal_compressed_size).sum();
        long totalBytes = columnMetaData.stream().mapToLong(ColumnMetaData::getTotal_uncompressed_size).sum();
        // Store metadata for later - RowGroup will be created after indexes are written
        rowGroupMetadataList.add(new RowGroupMetadata(columnMetaData, totalCompressedBytes, totalBytes, rows, fileOffset));
    }

    private static Slice serializeFooter(FileMetaData fileMetaData)
            throws IOException
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(40);
        Util.writeFileMetaData(fileMetaData, dynamicSliceOutput);
        return dynamicSliceOutput.slice();
    }

    private static org.apache.parquet.format.ColumnChunk toColumnChunk(ColumnMetaData metaData)
    {
        // TODO Not sure whether file_offset is used
        org.apache.parquet.format.ColumnChunk columnChunk = new org.apache.parquet.format.ColumnChunk(0);
        columnChunk.setMeta_data(metaData);
        return columnChunk;
    }

    @VisibleForTesting
    static String formatCreatedBy(String trinoVersion)
    {
        // Add "(build n/a)" suffix to satisfy Parquet's VersionParser expectations
        // Apache Hive will skip timezone conversion if createdBy does not start with parquet-mr
        // https://github.com/apache/hive/blob/67ef629486ba38b1d3e0f400bee0073fa3c4e989/ql/src/java/org/apache/hadoop/hive/ql/io/parquet/ParquetRecordReaderBase.java#L154
        return "parquet-mr-trino version " + trinoVersion + " (build n/a)";
    }

    private void initColumnWriters()
    {
        this.columnWriters = ParquetWriters.getColumnWriters(
                messageType,
                primitiveTypes,
                compressionCodec,
                writerOption,
                parquetTimeZone);
    }

    private static class RowGroupMetadata
    {
        private final List<ColumnMetaData> columnMetaData;
        private final long totalCompressedBytes;
        private final long totalUncompressedBytes;
        private final int rows;
        private final long fileOffset;

        private RowGroupMetadata(
                List<ColumnMetaData> columnMetaData,
                long totalCompressedBytes,
                long totalUncompressedBytes,
                int rows,
                long fileOffset)
        {
            this.columnMetaData = requireNonNull(columnMetaData, "columnMetaData is null");
            this.totalCompressedBytes = totalCompressedBytes;
            this.totalUncompressedBytes = totalUncompressedBytes;
            this.rows = rows;
            this.fileOffset = fileOffset;
        }
    }

    private static class IndexOffsets
    {
        private final long offset;
        private final int length;

        private IndexOffsets(long offset, int length)
        {
            this.offset = offset;
            this.length = length;
        }
    }

    private static class FileFooter
    {
        private final MessageType messageType;
        private final String createdBy;
        private final Optional<DateTimeZone> parquetTimeZone;

        @Nullable
        private ImmutableList.Builder<RowGroup> rowGroupBuilder = ImmutableList.builder();

        private FileFooter(MessageType messageType, String createdBy, Optional<DateTimeZone> parquetTimeZone)
        {
            this.messageType = messageType;
            this.createdBy = createdBy;
            this.parquetTimeZone = parquetTimeZone;
        }

        public void addRowGroup(RowGroup rowGroup)
        {
            checkState(rowGroupBuilder != null, "rowGroupBuilder is null");
            rowGroupBuilder.add(rowGroup);
        }

        public FileMetaData createFileMetadata()
        {
            checkState(rowGroupBuilder != null, "rowGroupBuilder is null");
            List<RowGroup> rowGroups = rowGroupBuilder.build();
            rowGroupBuilder = null;
            long totalRows = rowGroups.stream().mapToLong(RowGroup::getNum_rows).sum();
            FileMetaData fileMetaData = new FileMetaData(
                    1,
                    MessageTypeConverter.toParquetSchema(messageType),
                    totalRows,
                    ImmutableList.copyOf(rowGroups));
            fileMetaData.setCreated_by(createdBy);
            // Added based on org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport
            parquetTimeZone.ifPresent(dateTimeZone -> fileMetaData.setKey_value_metadata(
                    ImmutableList.of(new KeyValue("writer.time.zone").setValue(dateTimeZone.getID()))));
            return fileMetaData;
        }
    }
}
