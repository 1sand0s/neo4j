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
package org.neo4j.bolt;

import static org.neo4j.bolt.protocol.common.connection.DefaultConnectionHintProvider.connectionHintProviderFunction;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import javax.net.ssl.SSLException;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connection.BoltConnectionFactory;
import org.neo4j.bolt.protocol.common.connection.ConnectionHintProvider;
import org.neo4j.bolt.protocol.common.connection.DefaultBoltConnectionFactory;
import org.neo4j.bolt.protocol.v40.BoltProtocolV40;
import org.neo4j.bolt.protocol.v40.bookmark.BookmarksParserV40;
import org.neo4j.bolt.protocol.v41.BoltProtocolV41;
import org.neo4j.bolt.protocol.v42.BoltProtocolV42;
import org.neo4j.bolt.protocol.v43.BoltProtocolV43;
import org.neo4j.bolt.protocol.v44.BoltProtocolV44;
import org.neo4j.bolt.runtime.scheduling.BoltSchedulerProvider;
import org.neo4j.bolt.runtime.scheduling.CachedThreadPoolExecutorFactory;
import org.neo4j.bolt.runtime.scheduling.ExecutorBoltSchedulerProvider;
import org.neo4j.bolt.runtime.scheduling.NettyThreadFactory;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.security.basic.BasicAuthentication;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.bolt.transport.BoltMemoryPool;
import org.neo4j.bolt.transport.Netty4LoggerFactory;
import org.neo4j.bolt.transport.NettyServer;
import org.neo4j.bolt.transport.NettyServer.ProtocolInitializer;
import org.neo4j.bolt.transport.SocketTransport;
import org.neo4j.buffer.CentralBufferMangerHolder;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SslSystemSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.CommonConnectorConfig;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.config.AuthConfigProvider;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.util.VisibleForTesting;

public class BoltServer extends LifecycleAdapter {

    @VisibleForTesting
    public static final PooledByteBufAllocator NETTY_BUF_ALLOCATOR =
            new PooledByteBufAllocator(PlatformDependent.directBufferPreferred());

    private final BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI;
    private final JobScheduler jobScheduler;
    private final ConnectorPortRegister connectorPortRegister;
    private final NetworkConnectionTracker connectionTracker;
    private final DatabaseIdRepository databaseIdRepository;
    private final Config config;
    private final SystemNanoClock clock;
    private final Monitors monitors;
    private final LogService logService;
    private final AuthManager externalAuthManager;
    private final AuthManager internalAuthManager;
    private final AuthManager loopbackAuthManager;
    private final MemoryPools memoryPools;
    private final DefaultDatabaseResolver defaultDatabaseResolver;
    private final CentralBufferMangerHolder centralBufferMangerHolder;
    private final TransactionManager transactionManager;
    private final ConnectionHintProvider connectionHintProvider;

    // edition specific dependencies are resolved dynamically
    private final DependencyResolver dependencyResolver;

    private final LifeSupport life = new LifeSupport();

    private BoltMemoryPool boltMemoryPool;

    public BoltServer(
            BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI,
            JobScheduler jobScheduler,
            ConnectorPortRegister connectorPortRegister,
            NetworkConnectionTracker connectionTracker,
            DatabaseIdRepository databaseIdRepository,
            Config config,
            SystemNanoClock clock,
            Monitors monitors,
            LogService logService,
            DependencyResolver dependencyResolver,
            AuthManager externalAuthManager,
            AuthManager internalAuthManager,
            AuthManager loopbackAuthManager,
            MemoryPools memoryPools,
            DefaultDatabaseResolver defaultDatabaseResolver,
            CentralBufferMangerHolder centralBufferMangerHolder,
            TransactionManager transactionManager) {
        this.boltGraphDatabaseManagementServiceSPI = boltGraphDatabaseManagementServiceSPI;
        this.jobScheduler = jobScheduler;
        this.connectorPortRegister = connectorPortRegister;
        this.connectionTracker = connectionTracker;
        this.databaseIdRepository = databaseIdRepository;
        this.config = config;
        this.clock = clock;
        this.monitors = monitors;
        this.logService = logService;
        this.dependencyResolver = dependencyResolver;
        this.externalAuthManager = externalAuthManager;
        this.internalAuthManager = internalAuthManager;
        this.loopbackAuthManager = loopbackAuthManager;
        this.memoryPools = memoryPools;
        this.defaultDatabaseResolver = defaultDatabaseResolver;
        this.centralBufferMangerHolder = centralBufferMangerHolder;
        this.transactionManager = transactionManager;
        this.connectionHintProvider = connectionHintProviderFunction.apply(config);
    }

    @Override
    public void init() {
        InternalLog log = logService.getInternalLog(BoltServer.class);

        boltMemoryPool = new BoltMemoryPool(memoryPools, NETTY_BUF_ALLOCATOR.metric());
        life.add(new BoltMemoryPoolLifeCycleAdapter(boltMemoryPool));

        InternalLoggerFactory.setDefaultFactory(new Netty4LoggerFactory(logService.getInternalLogProvider()));

        BoltSchedulerProvider boltSchedulerProvider = life.setLast(new ExecutorBoltSchedulerProvider(
                config, new CachedThreadPoolExecutorFactory(), jobScheduler, logService));
        BoltConnectionFactory boltConnectionFactory =
                createConnectionFactory(config, boltSchedulerProvider, logService, clock);

        var customBookmarkParser = boltGraphDatabaseManagementServiceSPI
                .getCustomBookmarkFormatParser()
                .orElse(CustomBookmarkFormatParser.DEFAULT);
        var bookmarksParser = new BookmarksParserV40(databaseIdRepository, customBookmarkParser);

        var protocolRegistry = BoltProtocolRegistry.builder()
                .register(new BoltProtocolV40(
                        bookmarksParser,
                        logService,
                        boltGraphDatabaseManagementServiceSPI,
                        defaultDatabaseResolver,
                        transactionManager,
                        clock))
                .register(new BoltProtocolV41(
                        bookmarksParser,
                        logService,
                        boltGraphDatabaseManagementServiceSPI,
                        defaultDatabaseResolver,
                        transactionManager,
                        clock))
                .register(new BoltProtocolV42(
                        bookmarksParser,
                        logService,
                        boltGraphDatabaseManagementServiceSPI,
                        defaultDatabaseResolver,
                        transactionManager,
                        clock))
                .register(new BoltProtocolV43(
                        bookmarksParser,
                        logService,
                        boltGraphDatabaseManagementServiceSPI,
                        defaultDatabaseResolver,
                        transactionManager,
                        clock))
                .register(new BoltProtocolV44(
                        bookmarksParser,
                        logService,
                        boltGraphDatabaseManagementServiceSPI,
                        defaultDatabaseResolver,
                        transactionManager,
                        clock))
                .build();

        if (config.get(CommonConnectorConfig.ocsp_stapling_enabled)) {
            enableOcspStapling();
        }

        ByteBufAllocator bufferAllocator = getBufferAllocator();

        if (config.get(BoltConnector.enabled)) {
            jobScheduler.setThreadFactory(Group.BOLT_NETWORK_IO, NettyThreadFactory::new);
            NettyServer nettyServer;

            var isNotReadReplica = config.get(GraphDatabaseSettings.mode) != GraphDatabaseSettings.Mode.READ_REPLICA;
            var loopbackProtocolInitializer = createLoopbackProtocolInitializer(
                    protocolRegistry,
                    boltConnectionFactory,
                    createAuthentication(loopbackAuthManager),
                    bufferAllocator);

            if (config.get(GraphDatabaseSettings.routing_enabled) && isNotReadReplica) {
                nettyServer = new NettyServer(
                        jobScheduler.threadFactory(Group.BOLT_NETWORK_IO),
                        createExternalProtocolInitializer(
                                protocolRegistry,
                                boltConnectionFactory,
                                createAuthentication(externalAuthManager),
                                log,
                                bufferAllocator),
                        createInternalProtocolInitializer(
                                protocolRegistry,
                                boltConnectionFactory,
                                createAuthentication(internalAuthManager),
                                bufferAllocator),
                        loopbackProtocolInitializer,
                        connectorPortRegister,
                        logService,
                        config);
            } else {
                nettyServer = new NettyServer(
                        jobScheduler.threadFactory(Group.BOLT_NETWORK_IO),
                        createExternalProtocolInitializer(
                                protocolRegistry,
                                boltConnectionFactory,
                                createAuthentication(externalAuthManager),
                                log,
                                bufferAllocator),
                        loopbackProtocolInitializer,
                        connectorPortRegister,
                        logService,
                        config);
            }

            life.add(nettyServer);
            log.info("Bolt server loaded");
        }

        life.init();
    }

    @Override
    public void start() throws Exception {
        life.start(); // init and start the nested lifecycle
    }

    @Override
    public void stop() throws Exception {
        life.stop(); // stop the nested lifecycle
    }

    @Override
    public void shutdown() {
        life.shutdown(); // shutdown the nested lifecycle
    }

    private BoltConnectionFactory createConnectionFactory(
            Config config, BoltSchedulerProvider schedulerProvider, LogService logService, Clock clock) {
        return new DefaultBoltConnectionFactory(schedulerProvider, config, logService, clock, monitors);
    }

    private ProtocolInitializer createInternalProtocolInitializer(
            BoltProtocolRegistry boltProtocolRegistry,
            BoltConnectionFactory connectionFactory,
            Authentication authentication,
            ByteBufAllocator bufferAllocator) {

        SslContext sslCtx = null;
        SslPolicyLoader sslPolicyLoader = dependencyResolver.resolveDependency(SslPolicyLoader.class);

        boolean requireEncryption = sslPolicyLoader.hasPolicyForSource(CLUSTER);

        if (requireEncryption) {
            try {
                sslCtx = sslPolicyLoader.getPolicy(CLUSTER).nettyServerContext();
            } catch (SSLException e) {
                throw new RuntimeException(
                        "Failed to initialize SSL encryption support, which is required to start this connector. "
                                + "Error was: " + e.getMessage(),
                        e);
            }
        }

        SocketAddress internalListenAddress;

        if (config.isExplicitlySet(GraphDatabaseSettings.routing_listen_address)) {
            internalListenAddress =
                    config.get(GraphDatabaseSettings.routing_listen_address).socketAddress();
        } else {
            // otherwise use same host as external connector but with default internal port
            internalListenAddress = new InetSocketAddress(
                    config.get(BoltConnector.listen_address).getHostname(),
                    config.get(GraphDatabaseSettings.routing_listen_address).getPort());
        }

        return new SocketTransport(
                BoltConnector.NAME,
                internalListenAddress,
                sslCtx,
                requireEncryption,
                logService.getInternalLogProvider(),
                boltProtocolRegistry,
                connectionFactory,
                connectionTracker,
                bufferAllocator,
                boltMemoryPool,
                authentication,
                dependencyResolver.resolveDependency(AuthConfigProvider.class),
                connectionHintProvider,
                config);
    }

    private ProtocolInitializer createLoopbackProtocolInitializer(
            BoltProtocolRegistry boltProtocolRegistry,
            BoltConnectionFactory connectionFactory,
            Authentication authentication,
            ByteBufAllocator bufferAllocator) {
        if (config.get(BoltConnectorInternalSettings.enable_loopback_auth)) {
            if (config.get(BoltConnectorInternalSettings.unsupported_loopback_listen_file) == null) {
                throw new IllegalArgumentException(
                        "A file has not been specified for use with the loopback domain socket.");
            }

            File unixSocketFile = new File(config.get(BoltConnectorInternalSettings.unsupported_loopback_listen_file)
                    .toString());

            if (unixSocketFile.exists()) // Check if the file does not exist before passing to netty to create it
            {
                if (config.get(BoltConnectorInternalSettings.unsupported_loopback_delete)) {
                    try {
                        Files.deleteIfExists(Path.of(unixSocketFile.getPath()));
                    } catch (IOException e) {
                        throw new IllegalStateException(
                                "Failed to delete loopback domain socket file '" + unixSocketFile + "': "
                                        + e.getMessage(),
                                e);
                    }
                } else {
                    throw new IllegalArgumentException("Loopback listen file: " + unixSocketFile + " already exists.");
                }
            }

            DomainSocketAddress loopbackListenAddress = new DomainSocketAddress(unixSocketFile);

            return new SocketTransport(
                    BoltConnectorInternalSettings.LOOPBACK_NAME,
                    loopbackListenAddress,
                    null,
                    false,
                    logService.getInternalLogProvider(),
                    boltProtocolRegistry,
                    connectionFactory,
                    connectionTracker,
                    bufferAllocator,
                    boltMemoryPool,
                    authentication,
                    dependencyResolver.resolveDependency(AuthConfigProvider.class),
                    connectionHintProvider,
                    config);
        } else {
            return null;
        }
    }

    private ProtocolInitializer createExternalProtocolInitializer(
            BoltProtocolRegistry boltProtocolRegistry,
            BoltConnectionFactory connectionFactory,
            Authentication authentication,
            InternalLog log,
            ByteBufAllocator bufferAllocator) {
        SslContext sslCtx;
        boolean requireEncryption;
        BoltConnector.EncryptionLevel encryptionLevel = config.get(BoltConnector.encryption_level);
        SslPolicyLoader sslPolicyLoader = dependencyResolver.resolveDependency(SslPolicyLoader.class);

        switch (encryptionLevel) {
            case REQUIRED:
                // Encrypted connections are mandatory.
                requireEncryption = true;
                sslCtx = createSslContext(sslPolicyLoader);
                break;
            case OPTIONAL:
                // Encrypted connections are optional.
                requireEncryption = false;
                sslCtx = createSslContext(sslPolicyLoader);
                break;
            case DISABLED:
                // Encryption is turned off.
                requireEncryption = false;
                sslCtx = null;
                break;
            default:
                // In the unlikely event that we happen to fall through to the default option here,
                // there is a mismatch between the BoltConnector.EncryptionLevel enum and the options
                // handled in this switch statement. In this case, we'll log a warning and default to
                // disabling encryption, since this mirrors the functionality introduced in 3.0.
                log.warn("Unhandled encryption level %s - assuming DISABLED.", encryptionLevel.name());
                requireEncryption = false;
                sslCtx = null;
                break;
        }

        SocketAddress listenAddress = config.get(BoltConnector.listen_address).socketAddress();
        Duration channelTimeout = config.get(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout);
        long maxMessageSize =
                config.get(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes);
        long maxWriteLockMillis = config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_max_duration)
                .toMillis();

        return new SocketTransport(
                BoltConnector.NAME,
                listenAddress,
                sslCtx,
                requireEncryption,
                logService.getInternalLogProvider(),
                boltProtocolRegistry,
                connectionFactory,
                connectionTracker,
                bufferAllocator,
                boltMemoryPool,
                authentication,
                dependencyResolver.resolveDependency(AuthConfigProvider.class),
                connectionHintProvider,
                config);
    }

    private ByteBufAllocator getBufferAllocator() {
        // check if there is a Netty buffer allocator managed centrally
        // such allocator has also memory management done centrally
        if (centralBufferMangerHolder.getNettyBufferAllocator() != null) {
            return centralBufferMangerHolder.getNettyBufferAllocator();
        }

        life.add(new BoltMemoryPoolLifeCycleAdapter(boltMemoryPool));
        return NETTY_BUF_ALLOCATOR;
    }

    private static SslContext createSslContext(SslPolicyLoader sslPolicyFactory) {
        try {
            if (!sslPolicyFactory.hasPolicyForSource(BOLT)) {
                throw new IllegalArgumentException("No SSL policy has been configured for Bolt server");
            }
            return sslPolicyFactory.getPolicy(BOLT).nettyServerContext();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize SSL encryption support, which is required to start this connector. "
                            + "Error was: " + e.getMessage(),
                    e);
        }
    }

    private void enableOcspStapling() {
        if (SslProvider.JDK.equals(config.get(SslSystemSettings.netty_ssl_provider))) {
            // currently the only way to enable OCSP server stapling for JDK is through this property
            System.setProperty("jdk.tls.server.enableStatusRequestExtension", "true");
        } else {
            throw new IllegalArgumentException("OCSP Server stapling can only be used with JDK ssl provider (see "
                    + SslSystemSettings.netty_ssl_provider.name() + ")");
        }
    }

    private static Authentication createAuthentication(AuthManager authManager) {
        return new BasicAuthentication(authManager);
    }

    private static class BoltMemoryPoolLifeCycleAdapter extends LifecycleAdapter {
        private final BoltMemoryPool pool;

        private BoltMemoryPoolLifeCycleAdapter(BoltMemoryPool pool) {
            this.pool = pool;
        }

        @Override
        public void shutdown() {
            pool.close();
        }
    }
}
