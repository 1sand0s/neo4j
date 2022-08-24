/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency;

import static java.lang.Long.max;
import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_tracking;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.EmptyVersionContextSupplier.EMPTY;
import static org.neo4j.kernel.impl.factory.DbmsInfo.TOOL;
import static org.neo4j.kernel.impl.index.schema.SchemaIndexExtensionLoader.instantiateExtensions;
import static org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory.defaultHeuristicPageCacheMemory;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onShutdown;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static org.neo4j.logging.log4j.LogConfig.createLoggerFromXmlConfig;
import static org.neo4j.logging.log4j.LogUtils.newLoggerBuilder;
import static org.neo4j.logging.log4j.LogUtils.newTemporaryXmlConfigBuilder;
import static org.neo4j.logging.log4j.LoggerTarget.ROOT_LOGGER;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.logging.LoggingReporterFactoryInvocationHandler;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMapFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.recovery.LogTailExtractor;
import org.neo4j.logging.DuplicatingLog;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MachineMemory;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.time.Clocks;

public class ConsistencyCheckService {
    private final Date timestamp;
    private final DatabaseLayout layout;
    private final Config config;
    private final OutputStream progressOutput;
    private final InternalLogProvider logProvider;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final boolean verbose;
    private final Path reportDir;
    private final ConsistencyFlags consistencyFlags;
    private final PageCacheTracer pageCacheTracer;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;

    public ConsistencyCheckService(DatabaseLayout layout) {
        this(
                new Date(),
                layout,
                Config.defaults(),
                null,
                NullLogProvider.getInstance(),
                new DefaultFileSystemAbstraction(),
                null,
                false,
                null,
                ConsistencyFlags.ALL,
                PageCacheTracer.NULL,
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY),
                EmptyMemoryTracker.INSTANCE);
    }

    private ConsistencyCheckService(
            Date timestamp,
            DatabaseLayout layout,
            Config config,
            OutputStream progressOutput,
            InternalLogProvider logProvider,
            FileSystemAbstraction fileSystem,
            PageCache pageCache,
            boolean verbose,
            Path reportDir,
            ConsistencyFlags consistencyFlags,
            PageCacheTracer pageCacheTracer,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        this.timestamp = timestamp;
        this.layout = layout;
        this.config = config;
        this.progressOutput = progressOutput;
        this.logProvider = logProvider;
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.verbose = verbose;
        this.reportDir = reportDir;
        this.consistencyFlags = consistencyFlags;
        this.pageCacheTracer = pageCacheTracer;
        this.contextFactory = contextFactory;
        this.memoryTracker = memoryTracker;
    }

    public ConsistencyCheckService with(CursorContextFactory contextFactory) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportDir,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    public ConsistencyCheckService with(Date timestamp) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportDir,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    public ConsistencyCheckService with(DatabaseLayout layout) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportDir,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    public ConsistencyCheckService with(Config config) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportDir,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    public ConsistencyCheckService with(OutputStream progressOutput) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportDir,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    public ConsistencyCheckService with(InternalLogProvider logProvider) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportDir,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    public ConsistencyCheckService with(FileSystemAbstraction fileSystem) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportDir,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    public ConsistencyCheckService with(PageCache pageCache) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportDir,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    public ConsistencyCheckService verbose(boolean verbose) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportDir,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    public ConsistencyCheckService with(Path reportDir) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportDir,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    public ConsistencyCheckService with(ConsistencyFlags consistencyFlags) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportDir,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    public ConsistencyCheckService with(PageCacheTracer pageCacheTracer) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportDir,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    public ConsistencyCheckService with(MemoryTracker memoryTracker) {
        return new ConsistencyCheckService(
                timestamp,
                layout,
                config,
                progressOutput,
                logProvider,
                fileSystem,
                pageCache,
                verbose,
                reportDir,
                consistencyFlags,
                pageCacheTracer,
                contextFactory,
                memoryTracker);
    }

    public Result runFullConsistencyCheck() throws ConsistencyCheckIncompleteException {
        var life = new LifeSupport();
        try {
            var config = this.config;
            var pageCache = this.pageCache;
            if (pageCache == null) {
                // Now that there's no existing page cache we have the opportunity to change that setting for the
                // benefit of a faster consistency check ahead of us. Ask the checker what the optimal amount of
                // available off-heap memory would be and change the page cache memory setting a bit in that direction.
                long availablePhysicalMemory = OsBeanUtil.getFreePhysicalMemory();
                if (availablePhysicalMemory != OsBeanUtil.VALUE_UNAVAILABLE) {
                    availablePhysicalMemory *=
                            config.get(GraphDatabaseInternalSettings.consistency_check_memory_limit_factor);
                    long optimalOffHeapMemory = calculateOptimalOffHeapMemoryForChecker();
                    // Check the configured page cache memory setting and potentially change it a bit to get closer to
                    // the optimal amount of off-heap for the checker

                    // [heap|pageCache|                        ]
                    var heapMemory = Runtime.getRuntime().maxMemory();
                    var pageCacheMemory = config.get(GraphDatabaseSettings.pagecache_memory);
                    pageCacheMemory = pageCacheMemory != null
                            ? pageCacheMemory
                            : defaultHeuristicPageCacheMemory(MachineMemory.DEFAULT);
                    var availableOffHeapMemory = availablePhysicalMemory - heapMemory - pageCacheMemory;
                    if (availableOffHeapMemory < optimalOffHeapMemory) {
                        // current: [heap|        pageCache      |          ]
                        // optimal: [heap|pageCache|                        ]
                        // Reduce the page cache memory setting, although not below 20% of what it was configured to
                        var newPageCacheMemory = max(
                                (long) (pageCacheMemory * 0.2D),
                                availablePhysicalMemory - optimalOffHeapMemory - heapMemory);
                        config = Config.newBuilder()
                                .fromConfig(config)
                                .set(GraphDatabaseSettings.pagecache_memory, newPageCacheMemory)
                                .build();
                        logProvider
                                .getLog(ConsistencyCheckService.class)
                                .info(
                                        "%s setting was tweaked from %s down to %s for better overall performance of "
                                                + "the consistency checker",
                                        GraphDatabaseSettings.pagecache_memory.name(),
                                        bytesToString(pageCacheMemory),
                                        bytesToString(newPageCacheMemory));
                    }
                }

                var jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
                life.add(onShutdown(jobScheduler::close));
                var pageCacheFactory = new ConfiguringPageCacheFactory(
                        fileSystem,
                        config,
                        pageCacheTracer,
                        logProvider.getLog(PageCache.class),
                        jobScheduler,
                        Clocks.nanoClock(),
                        new MemoryPools(config.get(memory_tracking)),
                        pageCacheConfig -> pageCacheConfig.faultLockStriping(1 << 11));
                pageCache = pageCacheFactory.getOrCreatePageCache();
                life.add(onShutdown(pageCache::close));
            }
            var reportDir = this.reportDir != null ? this.reportDir : defaultReportDir(config);

            config.set(GraphDatabaseSettings.pagecache_warmup_enabled, false);

            // assert recovered
            var storageEngineFactory =
                    StorageEngineFactory.selectStorageEngine(fileSystem, layout).orElseThrow();
            final var databaseLayout = storageEngineFactory.formatSpecificDatabaseLayout(layout);
            assertRecovered(databaseLayout, pageCache, config, fileSystem, memoryTracker);

            assertSupportedFormat(
                    databaseLayout, config, fileSystem, pageCache, storageEngineFactory, logProvider, contextFactory);

            // instantiate the inconsistencies report logging
            var outLog = logProvider.getLog(getClass());
            var reportFile = chooseReportPath(reportDir);
            var reportLogProvider = new Log4jLogProvider(createLoggerFromXmlConfig(
                    fileSystem,
                    newTemporaryXmlConfigBuilder(fileSystem)
                            .withLogger(newLoggerBuilder(ROOT_LOGGER, reportFile)
                                    .withLevel(Level.INFO)
                                    .createOnDemand()
                                    .withCategory(false)
                                    .build())
                            .create(),
                    false,
                    config::configStringLookup));
            life.add(onShutdown(reportLogProvider::close));
            var reportLog = reportLogProvider.getLog(getClass());
            var log = new DuplicatingLog(outLog, reportLog);

            // instantiate kernel extensions and the StaticIndexProviderMapFactory thing
            var jobScheduler = life.add(JobSchedulerFactory.createInitialisedScheduler());
            var recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.ignore();
            var monitors = new Monitors();
            var tokenHolders = storageEngineFactory.loadReadOnlyTokens(
                    fileSystem, databaseLayout, config, pageCache, pageCacheTracer, true, contextFactory);
            var extensions = life.add(instantiateExtensions(
                    databaseLayout,
                    fileSystem,
                    config,
                    new SimpleLogService(logProvider),
                    pageCache,
                    jobScheduler,
                    recoveryCleanupWorkCollector,
                    TOOL,
                    HostedOnMode.SINGLE,
                    monitors,
                    tokenHolders,
                    pageCacheTracer,
                    readOnly()));
            var indexProviders = life.add(StaticIndexProviderMapFactory.create(
                    life,
                    config,
                    pageCache,
                    fileSystem,
                    new SimpleLogService(logProvider),
                    monitors,
                    readOnly(),
                    HostedOnMode.SINGLE,
                    recoveryCleanupWorkCollector,
                    databaseLayout,
                    tokenHolders,
                    jobScheduler,
                    contextFactory,
                    pageCacheTracer,
                    extensions));

            // do the consistency check
            life.start();
            var numberOfThreads = defaultConsistencyCheckThreadsNumber();
            var memoryLimitLeewayFactor =
                    config.get(GraphDatabaseInternalSettings.consistency_check_memory_limit_factor);
            var summary = new ConsistencySummaryStatistics();

            if (consistencyFlags.checkIndexStructure()) {
                var openOptions =
                        storageEngineFactory.getStoreOpenOptions(fileSystem, pageCache, databaseLayout, contextFactory);
                var statisticsStore = getStatisticStore(
                        pageCache,
                        databaseLayout,
                        recoveryCleanupWorkCollector,
                        contextFactory,
                        pageCacheTracer,
                        openOptions);
                life.add(statisticsStore);
                consistencyCheckOnStatisticsStore(log, summary, statisticsStore);
            }

            var logTailExtractor =
                    new LogTailExtractor(fileSystem, pageCache, config, storageEngineFactory, DatabaseTracers.EMPTY);

            try {
                storageEngineFactory.consistencyCheck(
                        fileSystem,
                        databaseLayout,
                        config,
                        pageCache,
                        indexProviders,
                        log,
                        summary,
                        numberOfThreads,
                        memoryLimitLeewayFactor,
                        progressOutput,
                        verbose,
                        consistencyFlags,
                        contextFactory,
                        pageCacheTracer,
                        logTailExtractor.getTailMetadata(databaseLayout, memoryTracker));
            } catch (Exception e) {
                throw new ConsistencyCheckIncompleteException(e);
            }

            if (!summary.isConsistent()) {
                log.warn("Inconsistencies found: " + summary);
                log.warn("See '%s' for a detailed consistency report.", reportFile);
                return Result.failure(reportFile, summary);
            }
            return Result.success(reportFile, summary);
        } finally {
            life.shutdown();
        }
    }

    private static void assertSupportedFormat(
            DatabaseLayout databaseLayout,
            Config config,
            FileSystemAbstraction fileSystem,
            PageCache pageCache,
            StorageEngineFactory storageEngineFactory,
            InternalLogProvider logProvider,
            CursorContextFactory contextFactory) {
        StoreVersionCheck storeVersionCheck = storageEngineFactory.versionCheck(
                fileSystem, databaseLayout, config, pageCache, new SimpleLogService(logProvider), contextFactory);

        try (var cursorContext = contextFactory.create("consistencyCheck")) {
            if (!storeVersionCheck.isCurrentStoreVersionFullySupported(cursorContext)) {
                throw new IllegalStateException(
                        "The store must be upgraded or migrated to a supported version before it is possible to "
                                + "check consistency");
            }
        }
    }

    /**
     * Starts a tiny page cache just to ask the store about rough number of entities in it. Based on that the storage can then decide
     * what the optimal amount of available off-heap memory should be when running the consistency check.
     */
    private long calculateOptimalOffHeapMemoryForChecker() throws ConsistencyCheckIncompleteException {
        try (JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
                PageCache tempPageCache = new ConfiguringPageCacheFactory(
                                fileSystem,
                                Config.defaults(GraphDatabaseSettings.pagecache_memory, mebiBytes(8)),
                                PageCacheTracer.NULL,
                                NullLog.getInstance(),
                                jobScheduler,
                                Clocks.nanoClock(),
                                new MemoryPools())
                        .getOrCreatePageCache()) {
            StorageEngineFactory storageEngineFactory =
                    StorageEngineFactory.selectStorageEngine(fileSystem, layout).orElseThrow();
            return storageEngineFactory.optimalAvailableConsistencyCheckerMemory(
                    fileSystem, layout, config, tempPageCache);
        } catch (Exception e) {
            throw new ConsistencyCheckIncompleteException(e);
        }
    }

    private static IndexStatisticsStore getStatisticStore(
            PageCache pageCache,
            DatabaseLayout layout,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            ImmutableSet<OpenOption> openOptions) {
        try {
            return new IndexStatisticsStore(
                    pageCache,
                    layout,
                    recoveryCleanupWorkCollector,
                    readOnly(),
                    contextFactory,
                    pageCacheTracer,
                    openOptions);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void consistencyCheckOnStatisticsStore(
            InternalLog log, ConsistencySummaryStatistics summary, ConsistencyCheckable checkable) {
        LoggingReporterFactoryInvocationHandler handler = new LoggingReporterFactoryInvocationHandler(log, true);
        ReporterFactory proxyFactory = new ReporterFactory(handler);

        checkable.consistencyCheck(proxyFactory, NULL_CONTEXT);
        summary.update("INDEX_STATISTICS", handler.errors(), handler.warnings());
    }

    private static void assertRecovered(
            DatabaseLayout databaseLayout,
            PageCache pageCache,
            Config config,
            FileSystemAbstraction fileSystem,
            MemoryTracker memoryTracker)
            throws ConsistencyCheckIncompleteException {
        try {
            if (isRecoveryRequired(
                    fileSystem,
                    pageCache,
                    databaseLayout,
                    config,
                    Optional.empty(),
                    memoryTracker,
                    DatabaseTracers.EMPTY)) {
                throw new IllegalStateException(joinAsLines(
                        "Active logical log detected, this might be a source of inconsistencies.",
                        "Please recover database.",
                        "To perform recovery please start database in single mode and perform clean shutdown."));
            }
        } catch (Exception e) {
            throw new ConsistencyCheckIncompleteException(e);
        }
    }

    private Path chooseReportPath(Path reportDir) {
        return reportDir.resolve(defaultLogFileName(timestamp));
    }

    private static Path defaultReportDir(Config tuningConfiguration) {
        return tuningConfiguration.get(GraphDatabaseSettings.logs_directory);
    }

    private static String defaultLogFileName(Date date) {
        return format("inconsistencies-%s.report", new SimpleDateFormat("yyyy-MM-dd.HH.mm.ss").format(date));
    }

    public static class Result {
        private final boolean successful;
        private final Path reportFile;
        private final ConsistencySummaryStatistics summary;

        public static Result failure(Path reportFile, ConsistencySummaryStatistics summary) {
            return new Result(false, reportFile, summary);
        }

        public static Result success(Path reportFile, ConsistencySummaryStatistics summary) {
            return new Result(true, reportFile, summary);
        }

        private Result(boolean successful, Path reportFile, ConsistencySummaryStatistics summary) {
            this.successful = successful;
            this.reportFile = reportFile;
            this.summary = summary;
        }

        public boolean isSuccessful() {
            return successful;
        }

        public Path reportFile() {
            return reportFile;
        }

        public ConsistencySummaryStatistics summary() {
            return summary;
        }
    }

    public static int defaultConsistencyCheckThreadsNumber() {
        return Runtime.getRuntime().availableProcessors();
    }
}
