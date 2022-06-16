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
package org.neo4j.kernel.impl.storemigration;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.batchimport.Configuration.defaultConfiguration;
import static org.neo4j.internal.recordstorage.RecordStorageEngineFactory.createMigrationTargetSchemaRuleAccess;
import static org.neo4j.internal.recordstorage.StoreTokens.allTokens;
import static org.neo4j.kernel.impl.storemigration.FileOperation.COPY;
import static org.neo4j.kernel.impl.storemigration.FileOperation.DELETE;
import static org.neo4j.kernel.impl.storemigration.FileOperation.MOVE;
import static org.neo4j.kernel.impl.storemigration.SchemaStoreMigration.getSchemaStoreMigration;
import static org.neo4j.kernel.impl.storemigration.StoreMigratorFileOperation.fileOperation;
import static org.neo4j.kernel.impl.transaction.log.LogTailMetadata.EMPTY_LOG_TAIL;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import org.eclipse.collections.api.factory.Sets;
import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.Monitor;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.Input.Estimates;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.ReadableGroups;
import org.neo4j.internal.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.ScanOnOpenOverwritingIdGeneratorFactory;
import org.neo4j.internal.id.ScanOnOpenReadOnlyIdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordNodeCursor;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.CommonDatabaseFile;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseFile;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.LegacyMetadataHandler;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.storemigration.SchemaStoreMigration.SchemaStoreMigrator;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.api.SchemaRule44;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.format.CapabilityType;
import org.neo4j.storageengine.api.format.Index44Compatibility;
import org.neo4j.storageengine.migration.AbstractStoreMigrationParticipant;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

/**
 * Migrates a {@link RecordStorageEngine} store from one version to another.
 * <p>
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the migration code is specific for the current upgrade and changes with each store format version.
 * <p>
 * Just one out of many potential participants in a migration.
 */
public class RecordStorageMigrator extends AbstractStoreMigrationParticipant {
    public static final String NAME = "Store files";

    private static final String RECORD_STORAGE_MIGRATION_TAG = "recordStorageMigration";
    private static final String NODE_CHUNK_MIGRATION_TAG = "nodeChunkMigration";
    private static final String RELATIONSHIP_CHUNK_MIGRATION_TAG = "relationshipChunkMigration";

    private final Config config;
    private final LogService logService;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final PageCacheTracer pageCacheTracer;
    private final JobScheduler jobScheduler;
    private final CursorContextFactory contextFactory;
    private final BatchImporterFactory batchImporterFactory;
    private final MemoryTracker memoryTracker;
    private final boolean forceBtreeIndexesToRange;

    public RecordStorageMigrator(
            FileSystemAbstraction fileSystem,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            LogService logService,
            JobScheduler jobScheduler,
            CursorContextFactory contextFactory,
            BatchImporterFactory batchImporterFactory,
            MemoryTracker memoryTracker,
            boolean forceBtreeIndexesToRange) {
        super(NAME);
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.config = config;
        this.logService = logService;
        this.jobScheduler = jobScheduler;
        this.pageCacheTracer = pageCacheTracer;
        this.contextFactory = contextFactory;
        this.batchImporterFactory = batchImporterFactory;
        this.memoryTracker = memoryTracker;
        this.forceBtreeIndexesToRange = forceBtreeIndexesToRange;
    }

    @Override
    public void migrate(
            DatabaseLayout directoryLayoutArg,
            DatabaseLayout migrationLayoutArg,
            ProgressReporter progressReporter,
            StoreVersion fromVersion,
            StoreVersion toVersion,
            IndexImporterFactory indexImporterFactory,
            LogTailMetadata tailMetadata)
            throws IOException, KernelException {
        RecordDatabaseLayout directoryLayout = RecordDatabaseLayout.convert(directoryLayoutArg);
        RecordDatabaseLayout migrationLayout = RecordDatabaseLayout.convert(migrationLayoutArg);
        RecordFormats oldFormat = ((RecordStoreVersion) fromVersion).getFormat();
        RecordFormats newFormat = ((RecordStoreVersion) toVersion).getFormat();

        boolean requiresDynamicStoreMigration = !newFormat.dynamic().equals(oldFormat.dynamic());
        boolean requiresPropertyMigration =
                !newFormat.property().equals(oldFormat.property()) || requiresDynamicStoreMigration;

        try (var cursorContext = contextFactory.create(RECORD_STORAGE_MIGRATION_TAG)) {
            SchemaStoreMigrator schemaStoreMigration = getSchemaStoreMigration(
                    oldFormat,
                    directoryLayout,
                    cursorContext,
                    requiresPropertyMigration,
                    forceBtreeIndexesToRange || SYSTEM_DATABASE_NAME.equals(directoryLayoutArg.getDatabaseName()),
                    config,
                    pageCache,
                    pageCacheTracer,
                    fileSystem,
                    contextFactory);

            schemaStoreMigration.assertCanMigrate();

            // Extract information about the last transaction from legacy neostore
            long lastTxId = tailMetadata.getLastCommittedTransaction().transactionId();
            TransactionId lastTxInfo = tailMetadata.getLastCommittedTransaction();
            LogPosition lastTxLogPosition = tailMetadata.getLastTransactionLogPosition();
            long checkpointLogVersion = tailMetadata.getCheckpointLogVersion();

            // The FORMAT capability also includes the format family so this comparison is enough
            if (!oldFormat.hasCompatibleCapabilities(newFormat, CapabilityType.FORMAT)) {
                // Some form of migration is required (a fallback/catch-all option)
                migrateWithBatchImporter(
                        directoryLayout,
                        migrationLayout,
                        lastTxId,
                        lastTxInfo.checksum(),
                        lastTxLogPosition.getLogVersion(),
                        lastTxLogPosition.getByteOffset(),
                        checkpointLogVersion,
                        progressReporter,
                        oldFormat,
                        newFormat,
                        requiresDynamicStoreMigration,
                        requiresPropertyMigration,
                        indexImporterFactory);
            }

            // First migration to 5.x - need to switch to new metadatastore and cleanup old index/constraints
            if (need50Migration(oldFormat)) {
                schemaStoreMigration.copyFilesInPreparationForMigration(fileSystem, directoryLayout, migrationLayout);

                IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory(
                        fileSystem, immediate(), pageCacheTracer, migrationLayout.getDatabaseName());
                IdGeneratorFactory srcIdGeneratorFactory = new ScanOnOpenReadOnlyIdGeneratorFactory();

                StoreFactory dstFactory = createStoreFactory(migrationLayout, newFormat, idGeneratorFactory);

                var metadata44 = LegacyMetadataHandler.readMetadata44FromStore(
                        pageCache, directoryLayout.metadataStore(), directoryLayout.getDatabaseName(), cursorContext);

                try (NeoStores dstStore = dstFactory.openNeoStores(
                                true,
                                StoreType.SCHEMA,
                                StoreType.PROPERTY,
                                StoreType.META_DATA,
                                StoreType.LABEL_TOKEN,
                                StoreType.RELATIONSHIP_TYPE_TOKEN,
                                StoreType.PROPERTY_KEY_TOKEN);
                        var dstCursors = new CachedStoreCursors(dstStore, cursorContext);
                        var dstAccess =
                                createMigrationTargetSchemaRuleAccess(dstStore, contextFactory, memoryTracker)) {
                    MetaDataStore metaDataStore = dstStore.getMetaDataStore();
                    metaDataStore.regenerateMetadata(
                            metadata44.storeId(),
                            metadata44.maybeExternalId() != null ? metadata44.maybeExternalId() : UUID.randomUUID(),
                            cursorContext);
                    if (metadata44.maybeDatabaseId() != null) {
                        metaDataStore.setDatabaseIdUuid(metadata44.maybeDatabaseId(), cursorContext);
                    }

                    dstStore.start(cursorContext);
                    var dstTokensHolders = createTokenHolders(dstStore, dstCursors);

                    schemaStoreMigration.migrate(dstAccess, dstTokensHolders);

                    try (var databaseFlushEvent = pageCacheTracer.beginDatabaseFlush()) {
                        dstStore.flush(databaseFlushEvent, cursorContext);
                    }
                }
            } else if (requiresPropertyMigration) {
                // Migrate schema store when changing format family
                schemaStoreMigration.copyFilesInPreparationForMigration(fileSystem, directoryLayout, migrationLayout);
                migrateSchemaStore(schemaStoreMigration, migrationLayout, newFormat, cursorContext, memoryTracker);
            }

            fileOperation(
                    COPY,
                    fileSystem,
                    directoryLayout,
                    migrationLayout,
                    singleton(CommonDatabaseFile.METADATA_STORE),
                    true,
                    false,
                    ExistingTargetStrategy.SKIP);
            var fieldAccess = MetaDataStore.getFieldAccess(
                    pageCache, migrationLayout.metadataStore(), migrationLayout.getDatabaseName(), cursorContext);

            StoreId oldStoreId = fieldAccess.readStoreId();
            long random = oldStoreId.getRandom();
            // Update store id if we have done a migration
            if (oldFormat.majorVersion() != newFormat.majorVersion()
                    || !oldFormat.getFormatFamily().equals(newFormat.getFormatFamily())) {
                random = new SecureRandom().nextLong();
            }

            StoreId newStoreId = new StoreId(
                    oldStoreId.getCreationTime(),
                    random,
                    RecordStorageEngineFactory.NAME,
                    newFormat.getFormatFamily().name(),
                    newFormat.majorVersion(),
                    newFormat.minorVersion());
            fieldAccess.writeStoreId(newStoreId);
        }
    }

    private void migrateWithBatchImporter(
            RecordDatabaseLayout sourceDirectoryStructure,
            RecordDatabaseLayout migrationDirectoryStructure,
            long lastTxId,
            int lastTxChecksum,
            long lastTxLogVersion,
            long lastTxLogByteOffset,
            long lastCheckpointLogVersion,
            ProgressReporter progressReporter,
            RecordFormats oldFormat,
            RecordFormats newFormat,
            boolean requiresDynamicStoreMigration,
            boolean requiresPropertyMigration,
            IndexImporterFactory indexImporterFactory)
            throws IOException {
        prepareBatchImportMigration(sourceDirectoryStructure, migrationDirectoryStructure, oldFormat, newFormat);

        try (NeoStores legacyStore = instantiateLegacyStore(oldFormat, sourceDirectoryStructure)) {
            Configuration importConfig = new Configuration.Overridden(defaultConfiguration(), config);
            AdditionalInitialIds additionalInitialIds = readAdditionalIds(
                    lastTxId, lastTxChecksum, lastTxLogVersion, lastTxLogByteOffset, lastCheckpointLogVersion);

            try (var storeCursors = new CachedStoreCursors(legacyStore, CursorContext.NULL_CONTEXT)) {
                // We have to make sure to keep the token ids if we're migrating properties/labels
                BatchImporter importer = batchImporterFactory.instantiate(
                        migrationDirectoryStructure,
                        fileSystem,
                        pageCacheTracer,
                        importConfig,
                        logService,
                        migrationBatchImporterMonitor(legacyStore, progressReporter, importConfig),
                        additionalInitialIds,
                        EMPTY_LOG_TAIL,
                        config,
                        Monitor.NO_MONITOR,
                        jobScheduler,
                        Collector.STRICT,
                        LogFilesInitializer.NULL,
                        indexImporterFactory,
                        memoryTracker,
                        contextFactory);
                InputIterable nodes = () -> legacyNodesAsInput(
                        legacyStore, requiresPropertyMigration, memoryTracker, storeCursors, contextFactory);
                InputIterable relationships = () -> legacyRelationshipsAsInput(
                        legacyStore, requiresPropertyMigration, contextFactory, memoryTracker, storeCursors);
                long propertyStoreSize = storeSize(legacyStore.getPropertyStore()) / 2
                        + storeSize(legacyStore.getPropertyStore().getStringStore()) / 2
                        + storeSize(legacyStore.getPropertyStore().getArrayStore()) / 2;
                Estimates estimates = Input.knownEstimates(
                        legacyStore.getNodeStore().getNumberOfIdsInUse(),
                        legacyStore.getRelationshipStore().getNumberOfIdsInUse(),
                        legacyStore.getPropertyStore().getNumberOfIdsInUse(),
                        legacyStore.getPropertyStore().getNumberOfIdsInUse(),
                        propertyStoreSize / 2,
                        propertyStoreSize / 2,
                        0 /*node labels left as 0 for now*/);
                importer.doImport(Input.input(nodes, relationships, IdType.ACTUAL, estimates, ReadableGroups.EMPTY));
            }

            // During migration the batch importer doesn't necessarily writes all entities, depending on
            // which stores needs migration. Node, relationship, relationship group stores are always written
            // anyways and cannot be avoided with the importer, but delete the store files that weren't written
            // (left empty) so that we don't overwrite those in the real store directory later.
            Collection<DatabaseFile> storesToDeleteFromMigratedDirectory = new ArrayList<>();
            storesToDeleteFromMigratedDirectory.add(CommonDatabaseFile.METADATA_STORE);
            if (!requiresPropertyMigration) {
                // We didn't migrate properties, so the property stores in the migrated store are just empty/bogus
                storesToDeleteFromMigratedDirectory.addAll(asList(
                        RecordDatabaseFile.PROPERTY_STORE,
                        RecordDatabaseFile.PROPERTY_STRING_STORE,
                        RecordDatabaseFile.PROPERTY_ARRAY_STORE));
            }
            if (!requiresDynamicStoreMigration) {
                // We didn't migrate labels (dynamic node labels) or any other dynamic store.
                storesToDeleteFromMigratedDirectory.addAll(asList(
                        RecordDatabaseFile.NODE_LABEL_STORE,
                        RecordDatabaseFile.LABEL_TOKEN_STORE,
                        RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE,
                        RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE,
                        RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE,
                        RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE,
                        RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                        RecordDatabaseFile.SCHEMA_STORE));
            }

            fileOperation(
                    DELETE,
                    fileSystem,
                    migrationDirectoryStructure,
                    migrationDirectoryStructure,
                    storesToDeleteFromMigratedDirectory,
                    true,
                    true,
                    null);
        }
    }

    private static long storeSize(CommonAbstractStore<? extends AbstractBaseRecord, ? extends StoreHeader> store) {
        return store.getNumberOfIdsInUse() * store.getRecordSize();
    }

    private NeoStores instantiateLegacyStore(RecordFormats format, RecordDatabaseLayout directoryStructure) {
        var storesToOpen = Arrays.stream(StoreType.values())
                .filter(storeType -> storeType != StoreType.META_DATA)
                .toArray(StoreType[]::new);
        return new StoreFactory(
                        directoryStructure,
                        config,
                        new ScanOnOpenReadOnlyIdGeneratorFactory(),
                        pageCache,
                        pageCacheTracer,
                        fileSystem,
                        format,
                        NullLogProvider.getInstance(),
                        contextFactory,
                        readOnly(),
                        EMPTY_LOG_TAIL,
                        Sets.immutable.empty())
                .openNeoStores(true, storesToOpen);
    }

    private void prepareBatchImportMigration(
            RecordDatabaseLayout sourceDirectoryStructure,
            RecordDatabaseLayout migrationStrcuture,
            RecordFormats oldFormat,
            RecordFormats newFormat)
            throws IOException {
        createStore(migrationStrcuture, newFormat);

        // We use the batch importer for migrating the data, and we use it in a special way where we only
        // rewrite the stores that have actually changed format. We know that to be node and relationship
        // stores. Although since the batch importer also populates the counts store, all labels need to
        // be read, i.e. both inlined and those existing in dynamic records. That's why we need to copy
        // that dynamic record store over before doing the "batch import".
        //   Copying this file just as-is assumes that the format hasn't change. If that happens we're in
        // a different situation, where we first need to migrate this file.

        // The token stores also need to be migrated because we use those as-is and ask for their high ids
        // when using the importer in the store migration scenario.
        RecordDatabaseFile[] storesFilesToMigrate = {
            RecordDatabaseFile.LABEL_TOKEN_STORE, RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE,
            RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE, RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
            RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE, RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE,
            RecordDatabaseFile.NODE_LABEL_STORE
        };
        if (newFormat.dynamic().equals(oldFormat.dynamic())) {
            fileOperation(
                    COPY,
                    fileSystem,
                    sourceDirectoryStructure,
                    migrationStrcuture,
                    Arrays.asList(storesFilesToMigrate),
                    true,
                    true,
                    ExistingTargetStrategy.OVERWRITE);
        } else {
            // Migrate all token stores and dynamic node label ids, keeping their ids intact
            DirectRecordStoreMigrator migrator =
                    new DirectRecordStoreMigrator(pageCache, fileSystem, config, contextFactory, pageCacheTracer);

            StoreType[] storesToMigrate = {
                StoreType.LABEL_TOKEN, StoreType.LABEL_TOKEN_NAME,
                StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY_KEY_TOKEN_NAME,
                StoreType.RELATIONSHIP_TYPE_TOKEN, StoreType.RELATIONSHIP_TYPE_TOKEN_NAME,
                StoreType.NODE_LABEL
            };

            // Migrate these stores silently because they are usually very small
            ProgressReporter progressReporter = ProgressReporter.SILENT;

            migrator.migrate(
                    sourceDirectoryStructure,
                    oldFormat,
                    migrationStrcuture,
                    newFormat,
                    progressReporter,
                    storesToMigrate,
                    StoreType.NODE);
        }

        // Since we'll be using these stores in the batch importer where we don't have this fine control over
        // IdGeneratorFactory
        // it's easier to just figure out highId and create simple id files of the current format at that highId.
        createStoreFactory(
                        migrationStrcuture,
                        newFormat,
                        new ScanOnOpenOverwritingIdGeneratorFactory(
                                fileSystem, pageCacheTracer, migrationStrcuture.getDatabaseName()))
                .openAllNeoStores()
                .close();
    }

    private void createStore(RecordDatabaseLayout migrationDirectoryStructure, RecordFormats newFormat) {
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory(
                fileSystem, immediate(), pageCacheTracer, migrationDirectoryStructure.getDatabaseName());
        createStoreFactory(migrationDirectoryStructure, newFormat, idGeneratorFactory)
                .openAllNeoStores(true)
                .close();
    }

    private StoreFactory createStoreFactory(
            RecordDatabaseLayout databaseLayout, RecordFormats formats, IdGeneratorFactory idGeneratorFactory) {
        return new StoreFactory(
                databaseLayout,
                config,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                fileSystem,
                formats,
                NullLogProvider.getInstance(),
                contextFactory,
                writable(),
                EMPTY_LOG_TAIL,
                immutable.empty());
    }

    private static AdditionalInitialIds readAdditionalIds(
            final long lastTxId,
            final int lastTxChecksum,
            final long lastTxLogVersion,
            final long lastTxLogByteOffset,
            long lastCheckpointLogVersion) {
        return new AdditionalInitialIds() {
            @Override
            public long lastCommittedTransactionId() {
                return lastTxId;
            }

            @Override
            public int lastCommittedTransactionChecksum() {
                return lastTxChecksum;
            }

            @Override
            public long lastCommittedTransactionLogVersion() {
                return lastTxLogVersion;
            }

            @Override
            public long lastCommittedTransactionLogByteOffset() {
                return lastTxLogByteOffset;
            }

            @Override
            public long checkpointLogVersion() {
                return lastCheckpointLogVersion;
            }
        };
    }

    private static ExecutionMonitor migrationBatchImporterMonitor(
            NeoStores legacyStore, final ProgressReporter progressReporter, Configuration config) {
        return new BatchImporterProgressMonitor(
                legacyStore.getNodeStore().getHighId(),
                legacyStore.getRelationshipStore().getHighId(),
                config,
                progressReporter);
    }

    private static InputIterator legacyRelationshipsAsInput(
            NeoStores legacyStore,
            boolean requiresPropertyMigration,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            StoreCursors storeCursors) {
        return new StoreScanAsInputIterator<>(legacyStore.getRelationshipStore()) {
            @Override
            public InputChunk newChunk() {
                var cursorContext = contextFactory.create(RELATIONSHIP_CHUNK_MIGRATION_TAG);
                return new RelationshipRecordChunk(
                        new RecordStorageReader(legacyStore),
                        requiresPropertyMigration,
                        cursorContext,
                        storeCursors,
                        memoryTracker);
            }
        };
    }

    private static InputIterator legacyNodesAsInput(
            NeoStores legacyStore,
            boolean requiresPropertyMigration,
            MemoryTracker memoryTracker,
            StoreCursors storeCursors,
            CursorContextFactory contextFactory) {
        return new StoreScanAsInputIterator<>(legacyStore.getNodeStore()) {
            @Override
            public InputChunk newChunk() {
                var cursorContext = contextFactory.create(NODE_CHUNK_MIGRATION_TAG);
                return new NodeRecordChunk(
                        new RecordStorageReader(legacyStore),
                        requiresPropertyMigration,
                        cursorContext,
                        storeCursors,
                        memoryTracker);
            }
        };
    }

    @Override
    public void moveMigratedFiles(
            DatabaseLayout migrationLayoutArg,
            DatabaseLayout directoryLayoutArg,
            StoreVersion versionToUpgradeFrom,
            StoreVersion versionToUpgradeTo)
            throws IOException {
        RecordDatabaseLayout directoryLayout = RecordDatabaseLayout.convert(directoryLayoutArg);
        RecordDatabaseLayout migrationLayout = RecordDatabaseLayout.convert(migrationLayoutArg);
        // Move the migrated ones into the store directory
        fileOperation(
                MOVE,
                fileSystem,
                migrationLayout,
                directoryLayout,
                Iterables.iterable(RecordDatabaseFile.allValues()),
                true, // allow to skip non existent source files
                true,
                ExistingTargetStrategy.OVERWRITE);

        RecordFormats oldFormat = ((RecordStoreVersion) versionToUpgradeFrom).getFormat();
        if (need50Migration(oldFormat)) {
            deleteBtreeIndexFiles(fileSystem, directoryLayout);
        }
    }

    /**
     * Migration of the schema store is invoked if the property store needs migration.
     */
    private void migrateSchemaStore(
            SchemaStoreMigrator schemaStoreMigration,
            RecordDatabaseLayout migrationLayout,
            RecordFormats newFormat,
            CursorContext cursorContext,
            MemoryTracker memoryTracker)
            throws IOException, KernelException {
        StoreFactory dstFactory = createStoreFactory(
                migrationLayout,
                newFormat,
                new ScanOnOpenOverwritingIdGeneratorFactory(
                        fileSystem, pageCacheTracer, migrationLayout.getDatabaseName()));

        // Token stores
        try (NeoStores dstStore = dstFactory.openNeoStores(
                        true,
                        StoreType.SCHEMA,
                        StoreType.PROPERTY_KEY_TOKEN,
                        StoreType.PROPERTY,
                        StoreType.PROPERTY_KEY_TOKEN_NAME,
                        StoreType.LABEL_TOKEN,
                        StoreType.LABEL_TOKEN_NAME,
                        StoreType.RELATIONSHIP_TYPE_TOKEN,
                        StoreType.RELATIONSHIP_TYPE_TOKEN_NAME);
                var dstCursors = new CachedStoreCursors(dstStore, cursorContext)) {
            dstStore.start(cursorContext);
            var dstTokensHolders = createTokenHolders(dstStore, dstCursors);
            try (SchemaRuleMigrationAccess dstAccess =
                    createMigrationTargetSchemaRuleAccess(dstStore, contextFactory, memoryTracker)) {
                schemaStoreMigration.migrate(dstAccess, dstTokensHolders);
            }

            try (var databaseFlushEvent = pageCacheTracer.beginDatabaseFlush()) {
                dstStore.flush(databaseFlushEvent, cursorContext);
            }
        }
    }

    private static void deleteBtreeIndexFiles(FileSystemAbstraction fs, RecordDatabaseLayout directoryLayout)
            throws IOException {
        fs.deleteRecursively(IndexDirectoryStructure.directoriesByProvider(directoryLayout.databaseDirectory())
                .forProvider(SchemaRule44.NATIVE_BTREE_10)
                .rootDirectory());
        fs.deleteRecursively(IndexDirectoryStructure.directoriesByProvider(directoryLayout.databaseDirectory())
                .forProvider(SchemaRule44.LUCENE_NATIVE_30)
                .rootDirectory());
    }

    static boolean need50Migration(RecordFormats oldFormat) {
        return oldFormat.hasCapability(Index44Compatibility.INSTANCE);
    }

    static TokenHolders createTokenHolders(NeoStores stores, CachedStoreCursors cursors) {
        TokenHolders tokenHolders = new TokenHolders(
                StoreTokens.createReadOnlyTokenHolder(TokenHolder.TYPE_PROPERTY_KEY),
                StoreTokens.createReadOnlyTokenHolder(TokenHolder.TYPE_LABEL),
                StoreTokens.createReadOnlyTokenHolder(TokenHolder.TYPE_RELATIONSHIP_TYPE));
        tokenHolders.setInitialTokens(allTokens(stores), cursors);
        return tokenHolders;
    }

    @Override
    public void cleanup(DatabaseLayout migrationLayout) throws IOException {}

    @Override
    public String toString() {
        return "Kernel StoreMigrator";
    }

    private static class NodeRecordChunk extends StoreScanChunk<RecordNodeCursor> {
        NodeRecordChunk(
                RecordStorageReader storageReader,
                boolean requiresPropertyMigration,
                CursorContext cursorContext,
                StoreCursors storeCursors,
                MemoryTracker memoryTracker) {
            super(
                    storageReader.allocateNodeCursor(cursorContext, storeCursors),
                    storageReader,
                    requiresPropertyMigration,
                    cursorContext,
                    storeCursors,
                    memoryTracker);
        }

        @Override
        protected void read(RecordNodeCursor cursor, long id) {
            cursor.single(id);
        }

        @Override
        protected void visitRecord(RecordNodeCursor record, InputEntityVisitor visitor) {
            visitor.id(record.entityReference());
            visitor.labelField(record.getLabelField());
            visitProperties(record, visitor);
        }
    }

    private static class RelationshipRecordChunk extends StoreScanChunk<StorageRelationshipScanCursor> {
        RelationshipRecordChunk(
                RecordStorageReader storageReader,
                boolean requiresPropertyMigration,
                CursorContext cursorContext,
                StoreCursors storeCursors,
                MemoryTracker memoryTracker) {
            super(
                    storageReader.allocateRelationshipScanCursor(cursorContext, storeCursors),
                    storageReader,
                    requiresPropertyMigration,
                    cursorContext,
                    storeCursors,
                    memoryTracker);
        }

        @Override
        protected void read(StorageRelationshipScanCursor cursor, long id) {
            cursor.single(id);
        }

        @Override
        protected void visitRecord(StorageRelationshipScanCursor record, InputEntityVisitor visitor) {
            visitor.startId(record.sourceNodeReference());
            visitor.endId(record.targetNodeReference());
            visitor.type(record.type());
            visitProperties(record, visitor);
        }
    }

    private static class BatchImporterProgressMonitor extends CoarseBoundedProgressExecutionMonitor {
        private final ProgressReporter progressReporter;

        BatchImporterProgressMonitor(
                long highNodeId,
                long highRelationshipId,
                Configuration configuration,
                ProgressReporter progressReporter) {
            super(highNodeId, highRelationshipId, configuration);
            this.progressReporter = progressReporter;
            this.progressReporter.start(total());
        }

        @Override
        protected void progress(long progress) {
            progressReporter.progress(progress);
        }
    }
}
