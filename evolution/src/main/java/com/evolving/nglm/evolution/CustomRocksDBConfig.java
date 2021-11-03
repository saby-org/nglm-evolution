package com.evolving.nglm.evolution;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CustomRocksDBConfig implements RocksDBConfigSetter {

    // Look here for the Kafka Streams defaults
    // https://github.com/apache/kafka/blob/2.3/streams/src/main/java/org/apache/kafka/streams/state/internals/RocksDBStore.java#L78

    private final String tableConfigBlockCacheSize = System.getenv().get("ROCKS_BLOCK_CACHE_SIZE_MB");
    private final Cache cache = tableConfigBlockCacheSize == null ? null : new LRUCache(Integer.parseInt(tableConfigBlockCacheSize) * 1024L * 1024L);
    private final String tableConfigBlockSize = System.getenv().get("ROCKS_BLOCK_SIZE_KB");
    private final String tableConfigCacheIndexAndFilterBlocks = System.getenv().get("ROCKS_CACHE_INDEX_AND_FILTER_BLOCKS");
    private final String tableConfigPinL0FilterAndIndexBlocksInCache = System.getenv().get("ROCKS_PIN_L0_FILTER_AND_INDEX_BLOCKS_IN_CACHE");
    private final String tableConfigNoBlockCache = System.getenv().get("ROCKS_NO_BLOCK_CACHE");
    private final String tableConfigBlockRestartInterval = System.getenv().get("ROCKS_BLOCK_RESTART_INTERVAL");
    private final CompressionType compressionType = resolveCompression(System.getenv().get("ROCKS_COMPRESSION_TYPE"));
    private final String writeBufferSize = System.getenv().get("ROCKS_WRITE_BUFFER_SIZE_MB");
    private final String maxWriteBufferNumber = System.getenv().get("ROCKS_MAX_WRITE_BUFFER_NUMBER");
    private final String minWriteBufferNumberToMerge = System.getenv().get("ROCKS_MIN_WRITE_BUFFER_NUMBER_TO_MERGE");
    private final CompactionStyle compactionStyle = resolveCompaction(System.getenv().get("ROCKS_COMPACTION_STYLE"));
    private final String prepareForBulkLoad = System.getenv().get("ROCKS_PREPARE_FOR_BULK_LOAD");
    private final String levelCompactionDynamicLevelBytes = System.getenv().get("ROCKS_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES");
    private final String targetFileSizeBase = System.getenv().get("ROCKS_TARGET_FILE_SIZE_BASE_MB");

    // Thread pools
    // set this to the fraction of the cores you want to use. E.g. 0.5 will use half of them.
    private final String increaseParallelismCoresFraction = System.getenv().get("ROCKS_INCREASE_PARALLELISM_CORES_FRACTION");
    private final int increaseParallelism = increaseParallelismCoresFraction == null ? -1 : (Float.valueOf(Float.parseFloat(increaseParallelismCoresFraction) * Runtime.getRuntime().availableProcessors())).intValue();
    private final String maxBackgroundFlushes = System.getenv().get("ROCKS_MAX_BACKGROUND_FLUSHES");
    private final String maxBackgroundCompactions = System.getenv().get("ROCKS_MAX_BACKGROUND_COMPACTIONS");
    private final String maxSubcompactions = System.getenv().get("ROCKS_MAX_SUBCOMPACTIONS");

    // Optimized for usage with RAM disk / tmpfs
    // https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#in-memory-database-with-full-functionalities
    private final String allowMmapReads = System.getenv().get("ROCKS_ALLOW_MMAP_READS");
    private final String allowMmapWrites = System.getenv().get("ROCKS_ALLOW_MMAP_WRITES");
    private final String useDirectReads = System.getenv().get("ROCKS_USE_DIRECT_READS");
    private final String useDirectIoForFlushAndCompaction = System.getenv().get("ROCKS_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION");
    private final String level0FileNumCompactionTrigger = System.getenv().get("ROCKS_LEVEL_0_FILE_NUM_COMPACTION_TRIGGER");
    private final String maxOpenFiles = System.getenv().get("ROCKS_MAX_OPEN_FILES");

    @Override
    public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
        // block based table config
        final BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        if (cache != null) {
            tableConfig.setBlockCache(cache);
        }
        if (tableConfigBlockSize != null) {
            tableConfig.setBlockSize(Long.parseLong(tableConfigBlockSize));
        }
        if (tableConfigCacheIndexAndFilterBlocks != null) {
            tableConfig.setCacheIndexAndFilterBlocks(Boolean.parseBoolean(tableConfigCacheIndexAndFilterBlocks));
        }
        if (tableConfigPinL0FilterAndIndexBlocksInCache != null) {
            tableConfig.setPinL0FilterAndIndexBlocksInCache(Boolean.parseBoolean(tableConfigPinL0FilterAndIndexBlocksInCache));
        }
        if (tableConfigBlockRestartInterval != null) {
            tableConfig.setBlockRestartInterval(Integer.parseInt(tableConfigBlockRestartInterval));
        }
        // disabling this recommended for RAM disk only
        if (tableConfigNoBlockCache != null) {
            tableConfig.setNoBlockCache(Boolean.parseBoolean(tableConfigNoBlockCache));
        }
        options.setTableFormatConfig(tableConfig);

        // general RocksDB config
        options.setCompressionType(compressionType);
        if (writeBufferSize != null) {
            options.setWriteBufferSize(Integer.parseInt(writeBufferSize) * 1024L * 1024L);
        }
        if (maxWriteBufferNumber != null) {
            options.setMaxWriteBufferNumber(Integer.parseInt(maxWriteBufferNumber));
        }
        if (minWriteBufferNumberToMerge != null) {
            options.setMinWriteBufferNumberToMerge(Integer.parseInt(minWriteBufferNumberToMerge));
        }
        if (compactionStyle != null) {
            options.setCompactionStyle(compactionStyle);
        }
        // Optimized for usage with RAM disk / tmpfs
        // https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#in-memory-database-with-full-functionalities
        if (allowMmapReads != null) {
            options.setAllowMmapReads(Boolean.parseBoolean(allowMmapReads));
        }
        if (allowMmapWrites != null) {
            options.setAllowMmapWrites(Boolean.parseBoolean(allowMmapWrites));
        }
        if (useDirectReads != null) {
            options.setUseDirectReads(Boolean.parseBoolean(useDirectReads));
        }
        if (useDirectIoForFlushAndCompaction != null) {
            options.setUseDirectIoForFlushAndCompaction(Boolean.parseBoolean(useDirectIoForFlushAndCompaction));
        }
        if (level0FileNumCompactionTrigger != null) {
            options.setLevel0FileNumCompactionTrigger(Integer.parseInt(level0FileNumCompactionTrigger));
        }
        if (maxOpenFiles != null) {
            options.setMaxOpenFiles(Integer.parseInt(maxOpenFiles));
        }
        // see here for comment on increaseParallelism
        // https://github.com/apache/kafka/blob/2.3/streams/src/main/java/org/apache/kafka/streams/state/internals/RocksDBStore.java#L78
        // apparently it doesn't increase the flash background processes, only compactions
        if (increaseParallelismCoresFraction != null) {
            options.setIncreaseParallelism(increaseParallelism);
        }
        if (maxBackgroundFlushes != null) {
            options.setMaxBackgroundFlushes(Integer.parseInt(maxBackgroundFlushes));
        }
        if (maxBackgroundCompactions != null) {
            options.setMaxBackgroundCompactions(Integer.parseInt(maxBackgroundCompactions));
        }
        if (maxSubcompactions != null) {
            options.setMaxSubcompactions(Integer.parseInt(maxSubcompactions));
        }
        if (levelCompactionDynamicLevelBytes != null) {
            options.setLevelCompactionDynamicLevelBytes(Boolean.parseBoolean(levelCompactionDynamicLevelBytes));
        }
        if (targetFileSizeBase != null) {
            options.setTargetFileSizeBase(Integer.parseInt(targetFileSizeBase) * 1024L * 1024L);
        }

        ///////
        /// WARNING ///
        /// Let this to be the last one in case that prepare bulk load overwrites other settings
        ///////
        if (prepareForBulkLoad != null && "true".equals(prepareForBulkLoad)) {
            options.prepareForBulkLoad();
        }
        dumpRocksDBSettings(storeName);
    }

    @Override
    public void close(String storeName, Options options) {
        if (cache != null) {
            cache.close();
        }
    }

    // Defaults to KafkaStreams default, no compression
    private static final CompressionType resolveCompression(String compressionTypeString) {
        if ("snappy".equals(compressionTypeString)) {
            return CompressionType.SNAPPY_COMPRESSION;
        } else if ("z".equals(compressionTypeString)) {
            return CompressionType.ZLIB_COMPRESSION;
        } else if ("bzip2".equals(compressionTypeString)) {
            return CompressionType.BZLIB2_COMPRESSION;
        } else if ("lz4".equals(compressionTypeString)) {
            return CompressionType.LZ4_COMPRESSION;
        } else if ("lz4hc".equals(compressionTypeString)) {
            return CompressionType.LZ4HC_COMPRESSION;
        } else if ("xpress".equals(compressionTypeString)) {
            return CompressionType.XPRESS_COMPRESSION;
        } else if ("zstd".equals(compressionTypeString)) {
            return CompressionType.ZSTD_COMPRESSION;
        }
        return CompressionType.NO_COMPRESSION;
    }

    // Defaults to KafkaStreams default, UNIVERSAL
    private static final CompactionStyle resolveCompaction(String compactionStyleString) {
        if ("level".equals(compactionStyleString)) {
            return CompactionStyle.LEVEL;
        } else if ("fifo".equals(compactionStyleString)) {
            return CompactionStyle.FIFO;
        }
        return CompactionStyle.UNIVERSAL;
    }

    private static final void dumpRocksDBSettings(String name) {
        final List<String> rocksEnvs = new ArrayList<>();
        final Map<String, String> env = System.getenv();
        for (Map.Entry<String, String> entry : env.entrySet()) {
            if(entry.getKey().startsWith("ROCKS_")) {
                rocksEnvs.add(entry.getKey() + ": " + entry.getValue());
            }
        }
        System.out.println(String.format("RocksDB %s: %s", name, String.join(", ", rocksEnvs)));
    }
}
