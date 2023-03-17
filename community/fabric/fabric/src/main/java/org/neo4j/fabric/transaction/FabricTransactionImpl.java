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
package org.neo4j.fabric.transaction;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionCommitFailed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import org.neo4j.cypher.internal.util.CancellationChecker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.eval.Catalog;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.executor.Exceptions;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.FabricKernelTransaction;
import org.neo4j.fabric.executor.FabricLocalExecutor;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.executor.SingleDbTransaction;
import org.neo4j.fabric.planning.StatementType;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.transaction.trace.TraceProvider;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.time.SystemNanoClock;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class FabricTransactionImpl
        implements FabricTransaction, CompositeTransaction, FabricTransaction.FabricExecutionContext {
    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    private final Set<ReadingTransaction> readingTransactions = ConcurrentHashMap.newKeySet();
    private final ReadWriteLock transactionLock = new ReentrantReadWriteLock();
    private final Lock nonExclusiveLock = transactionLock.readLock();
    private final Lock exclusiveLock = transactionLock.writeLock();
    private final FabricTransactionInfo transactionInfo;
    private final TransactionBookmarkManager bookmarkManager;
    private final Catalog catalogSnapshot;
    private final SystemNanoClock clock;
    private final ErrorReporter errorReporter;
    private final TransactionManager transactionManager;
    private final long id;
    private final FabricRemoteExecutor.RemoteTransactionContext remoteTransactionContext;
    private final FabricLocalExecutor.LocalTransactionContext localTransactionContext;
    private final AtomicReference<StatementType> statementType = new AtomicReference<>();
    private State state = State.OPEN;
    private TerminationMark terminationMark;

    private SingleDbTransaction writingTransaction;
    private final LocationCache locationCache;

    private final TransactionInitializationTrace initializationTrace;

    private final FabricKernelTransaction kernelTransaction;

    private final Procedures contextlessProcedures;

    FabricTransactionImpl(
            FabricTransactionInfo transactionInfo,
            TransactionBookmarkManager bookmarkManager,
            FabricRemoteExecutor remoteExecutor,
            FabricLocalExecutor localExecutor,
            FabricProcedures contextlessProcedures,
            ErrorReporter errorReporter,
            TransactionManager transactionManager,
            Catalog catalogSnapshot,
            CatalogManager catalogManager,
            Boolean inCompositeContext,
            SystemNanoClock clock,
            TraceProvider traceProvider) {
        this.transactionInfo = transactionInfo;
        this.errorReporter = errorReporter;
        this.transactionManager = transactionManager;
        this.bookmarkManager = bookmarkManager;
        this.catalogSnapshot = catalogSnapshot;
        this.clock = clock;
        this.id = ID_GENERATOR.incrementAndGet();
        this.initializationTrace = traceProvider.getTraceInfo();
        this.contextlessProcedures = contextlessProcedures;

        this.locationCache = new LocationCache(catalogManager, transactionInfo);

        try {
            remoteTransactionContext = remoteExecutor.startTransactionContext(this, transactionInfo, bookmarkManager);
            localTransactionContext = localExecutor.startTransactionContext(this, transactionInfo, bookmarkManager);
            DatabaseReference sessionDatabaseReference = getSessionDatabaseReference();
            if (inCompositeContext) {
                var graph = catalogSnapshot.resolveGraphByNameString(
                        sessionDatabaseReference.alias().name());
                var location = this.locationOf(graph, false);
                kernelTransaction = localTransactionContext.getOrCreateTx(
                        (Location.Local) location, TransactionMode.DEFINITELY_READ, true);
            } else {
                kernelTransaction = null;
            }
        } catch (RuntimeException e) {
            // the exception with stack trace will be logged by Bolt's ErrorReporter
            throw Exceptions.transform(Status.Transaction.TransactionStartFailed, e);
        }
    }

    @Override
    public Catalog getCatalogSnapshot() {
        return catalogSnapshot;
    }

    @Override
    public FabricTransactionInfo getTransactionInfo() {
        return transactionInfo;
    }

    @Override
    public FabricRemoteExecutor.RemoteTransactionContext getRemote() {
        return remoteTransactionContext;
    }

    @Override
    public FabricLocalExecutor.LocalTransactionContext getLocal() {
        return localTransactionContext;
    }

    @Override
    public void validateStatementType(StatementType type) {
        boolean wasNull = statementType.compareAndSet(null, type);
        if (!wasNull) {
            var oldType = statementType.get();
            if (oldType != type) {
                var queryAfterQuery = type.isQuery() && oldType.isQuery();
                var readQueryAfterSchema = type.isReadQuery() && oldType.isSchemaCommand();
                var schemaAfterReadQuery = type.isSchemaCommand() && oldType.isReadQuery();
                var allowedCombination = queryAfterQuery || readQueryAfterSchema || schemaAfterReadQuery;
                if (allowedCombination) {
                    var writeQueryAfterReadQuery = queryAfterQuery && !type.isReadQuery() && oldType.isReadQuery();
                    var upgrade = writeQueryAfterReadQuery || schemaAfterReadQuery;
                    if (upgrade) {
                        statementType.set(type);
                    }
                } else {
                    throw new FabricException(
                            Status.Transaction.ForbiddenDueToTransactionType,
                            "Tried to execute %s after executing %s",
                            type,
                            oldType);
                }
            }
        }
    }

    public boolean isSchemaTransaction() {
        var type = statementType.get();
        return type != null && type.isSchemaCommand();
    }

    @Override
    public DatabaseReference getSessionDatabaseReference() {
        return transactionInfo.getSessionDatabaseReference();
    }

    @Override
    public Location locationOf(Catalog.Graph graph, Boolean requireWritable) {
        return locationCache.locationOf(graph, requireWritable);
    }

    @Override
    public void commit() {
        exclusiveLock.lock();
        try {
            if (state == State.TERMINATED) {
                // Wait for all children to be rolled back. Ignore errors
                doRollbackAndIgnoreErrors(SingleDbTransaction::rollback);
                throw new TransactionTerminatedException(terminationMark.getReason());
            }

            if (state == State.CLOSED) {
                throw new FabricException(TransactionCommitFailed, "Trying to commit closed transaction");
            }

            state = State.CLOSED;

            var allFailures = new ArrayList<ErrorRecord>();

            try {
                doOnChildren(readingTransactions, null, SingleDbTransaction::commit)
                        .forEach(error ->
                                allFailures.add(new ErrorRecord("Failed to commit a child read transaction", error)));

                if (!allFailures.isEmpty()) {
                    doOnChildren(List.of(), writingTransaction, SingleDbTransaction::rollback)
                            .forEach(error -> allFailures.add(
                                    new ErrorRecord("Failed to rollback a child write transaction", error)));
                } else {
                    doOnChildren(List.of(), writingTransaction, SingleDbTransaction::commit)
                            .forEach(error -> allFailures.add(
                                    new ErrorRecord("Failed to commit a child write transaction", error)));
                }
            } catch (Exception e) {
                allFailures.add(new ErrorRecord("Failed to commit composite transaction", commitFailedError()));
            } finally {
                closeContextsAndRemoveTransaction();
            }

            throwIfNonEmpty(allFailures, this::commitFailedError);
        } finally {
            exclusiveLock.unlock();
        }
    }

    @Override
    public void rollback() {
        exclusiveLock.lock();
        try {
            // guard against someone calling rollback after 'begin' failure
            if (remoteTransactionContext == null && localTransactionContext == null) {
                return;
            }

            if (state == State.TERMINATED) {
                // Wait for all children to be rolled back. Ignore errors
                doRollbackAndIgnoreErrors(SingleDbTransaction::rollback);
                return;
            }

            if (state == State.CLOSED) {
                return;
            }

            state = State.CLOSED;
            doRollback(SingleDbTransaction::rollback);
        } finally {
            exclusiveLock.unlock();
        }
    }

    private void doRollback(Function<SingleDbTransaction, Mono<Void>> operation) {
        var allFailures = new ArrayList<ErrorRecord>();

        try {
            doOnChildren(readingTransactions, writingTransaction, operation)
                    .forEach(
                            error -> allFailures.add(new ErrorRecord("Failed to rollback a child transaction", error)));
        } catch (Exception e) {
            allFailures.add(new ErrorRecord("Failed to rollback composite transaction", rollbackFailedError()));
        } finally {
            closeContextsAndRemoveTransaction();
        }

        throwIfNonEmpty(allFailures, this::rollbackFailedError);
    }

    private void doRollbackAndIgnoreErrors(Function<SingleDbTransaction, Mono<Void>> operation) {
        try {
            doOnChildren(readingTransactions, writingTransaction, operation);
        } finally {
            closeContextsAndRemoveTransaction();
        }
    }

    private void closeContextsAndRemoveTransaction() {
        remoteTransactionContext.close();
        localTransactionContext.close();
        transactionManager.removeTransaction(this);
    }

    private void terminateChildren(Status reason) {
        var allFailures = new ArrayList<ErrorRecord>();
        try {
            doOnChildren(
                            readingTransactions,
                            writingTransaction,
                            singleDbTransaction -> singleDbTransaction.terminate(reason))
                    .forEach(error ->
                            allFailures.add(new ErrorRecord("Failed to terminate a child transaction", error)));
        } catch (Exception e) {
            allFailures.add(new ErrorRecord("Failed to terminate composite transaction", terminationFailedError()));
        }
        throwIfNonEmpty(allFailures, this::terminationFailedError);
    }

    private static List<Throwable> doOnChildren(
            Iterable<ReadingTransaction> readingTransactions,
            SingleDbTransaction writingTransaction,
            Function<SingleDbTransaction, Mono<Void>> operation) {
        var failures = Flux.fromIterable(readingTransactions)
                .map(txWrapper -> txWrapper.singleDbTransaction)
                .concatWith(Mono.justOrEmpty(writingTransaction))
                .flatMap(tx -> catchErrors(operation.apply(tx)))
                .collectList()
                .block();

        return failures == null ? List.of() : failures;
    }

    private static Mono<Throwable> catchErrors(Mono<Void> action) {
        return action.flatMap(v -> Mono.<Throwable>empty()).onErrorResume(Mono::just);
    }

    private void throwIfNonEmpty(List<ErrorRecord> failures, Supplier<FabricException> genericException) {
        if (!failures.isEmpty()) {
            var exception = genericException.get();
            if (failures.size() == 1) {
                // Nothing is logged if there is just one error, because it will be logged by Bolt
                // and the log would contain two lines reporting the same thing without any additional info.
                throw Exceptions.transform(exception.status(), failures.get(0).error);
            } else {
                failures.forEach(failure -> exception.addSuppressed(failure.error));
                failures.forEach(failure -> errorReporter.report(failure.message, failure.error, exception.status()));
                throw exception;
            }
        }
    }

    @Override
    public StatementResult execute(Function<FabricExecutionContext, StatementResult> runLogic) {
        checkTransactionOpenForStatementExecution();

        try {
            return runLogic.apply(this);
        } catch (RuntimeException e) {
            // the exception with stack trace will be logged by Bolt's ErrorReporter
            rollback();
            throw Exceptions.transform(Status.Statement.ExecutionFailed, e);
        }
    }

    private void checkTransactionOpenForStatementExecution() {
        if (state == State.TERMINATED) {
            throw new TransactionTerminatedException(terminationMark.getReason());
        }

        if (state == State.CLOSED) {
            throw new FabricException(
                    Status.Statement.ExecutionFailed, "Trying to execute query in a closed transaction");
        }
    }

    public boolean isLocal() {
        return remoteTransactionContext.isEmptyContext();
    }

    @Override
    public boolean isOpen() {
        return state == State.OPEN;
    }

    @Override
    public void markForTermination(Status reason) {
        // While state is open, take the lock by polling.
        // We do this to re-check state, which could be set by another thread committing or rolling back.
        while (true) {
            try {
                if (state != State.OPEN) {
                    return;
                } else {
                    if (exclusiveLock.tryLock(100, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw terminationFailedError();
            }
        }

        try {
            if (state != State.OPEN) {
                return;
            }

            terminationMark = new TerminationMark(reason, clock.nanos());
            state = State.TERMINATED;

            terminateChildren(reason);
        } finally {
            exclusiveLock.unlock();
        }
    }

    @Override
    public Optional<TerminationMark> getTerminationMark() {
        return Optional.ofNullable(terminationMark);
    }

    @Override
    public TransactionBookmarkManager getBookmarkManager() {
        return bookmarkManager;
    }

    @Override
    public void setMetaData(Map<String, Object> txMeta) {
        transactionInfo.setMetaData(txMeta);
        for (InternalTransaction internalTransaction : getInternalTransactions()) {
            internalTransaction.setMetaData(txMeta);
        }
    }

    @Override
    public <TX extends SingleDbTransaction> TX startWritingTransaction(
            Location location, Supplier<TX> writeTransactionSupplier) throws FabricException {
        exclusiveLock.lock();
        try {
            checkTransactionOpenForStatementExecution();

            if (writingTransaction != null) {
                throw multipleWriteError(location);
            }

            var tx = writeTransactionSupplier.get();
            writingTransaction = tx;
            return tx;
        } finally {
            exclusiveLock.unlock();
        }
    }

    @Override
    public <TX extends SingleDbTransaction> TX startReadingTransaction(Supplier<TX> readingTransactionSupplier)
            throws FabricException {
        return startReadingTransaction(false, readingTransactionSupplier);
    }

    @Override
    public <TX extends SingleDbTransaction> TX startReadingOnlyTransaction(Supplier<TX> readingTransactionSupplier)
            throws FabricException {
        return startReadingTransaction(true, readingTransactionSupplier);
    }

    public ExecutingQuery.TransactionBinding transactionBinding() throws FabricException {
        if (kernelTransaction == null) {
            return null;
        }
        DatabaseReference sessionDatabaseReference = getSessionDatabaseReference();
        NamedDatabaseId namedDbId =
                DatabaseIdFactory.from(sessionDatabaseReference.alias().name(), sessionDatabaseReference.id());

        Long transactionId = kernelTransaction.transactionId();
        return new ExecutingQuery.TransactionBinding(namedDbId, () -> 0L, () -> 0L, () -> 0L, transactionId);
    }

    public Procedures contextlessProcedures() {
        return contextlessProcedures;
    }

    private <TX extends SingleDbTransaction> TX startReadingTransaction(
            boolean readOnly, Supplier<TX> readingTransactionSupplier) throws FabricException {
        nonExclusiveLock.lock();
        try {
            checkTransactionOpenForStatementExecution();

            var tx = readingTransactionSupplier.get();
            readingTransactions.add(new ReadingTransaction(tx, readOnly));
            return tx;
        } finally {
            nonExclusiveLock.unlock();
        }
    }

    @Override
    public <TX extends SingleDbTransaction> void upgradeToWritingTransaction(TX writingTransaction)
            throws FabricException {
        if (this.writingTransaction == writingTransaction) {
            return;
        }

        exclusiveLock.lock();
        try {
            if (this.writingTransaction == writingTransaction) {
                return;
            }

            if (this.writingTransaction != null) {
                throw multipleWriteError(writingTransaction.getLocation());
            }

            ReadingTransaction readingTransaction = readingTransactions.stream()
                    .filter(readingTx -> readingTx.singleDbTransaction == writingTransaction)
                    .findAny()
                    .orElseThrow(
                            () -> new IllegalArgumentException("The supplied transaction has not been registered"));

            if (readingTransaction.readingOnly) {
                throw new IllegalStateException("Upgrading reading-only transaction to a writing one is not allowed");
            }

            readingTransactions.remove(readingTransaction);
            this.writingTransaction = readingTransaction.singleDbTransaction;
        } finally {
            exclusiveLock.unlock();
        }
    }

    @Override
    public void childTransactionTerminated(Status reason) {
        if (state != State.OPEN) {
            return;
        }

        markForTermination(reason);
    }

    @Override
    public CancellationChecker cancellationChecker() {
        return this::checkTransactionOpenForStatementExecution;
    }

    private FabricException multipleWriteError(Location attempt) {
        return new FabricException(
                Status.Statement.AccessMode,
                "Writing to more than one database per transaction is not allowed. Attempted write to %s, currently writing to %s",
                attempt.databaseReference().toPrettyString(),
                writingTransaction.getLocation().databaseReference().toPrettyString());
    }

    private FabricException commitFailedError() {
        return new FabricException(TransactionCommitFailed, "Failed to commit composite transaction %d", id);
    }

    private FabricException rollbackFailedError() {
        return new FabricException(
                Status.Transaction.TransactionRollbackFailed, "Failed to rollback composite transaction %d", id);
    }

    private FabricException terminationFailedError() {
        return new FabricException(
                Status.Transaction.TransactionTerminationFailed, "Failed to terminate composite transaction %d", id);
    }

    public long getId() {
        return id;
    }

    public TransactionInitializationTrace getInitializationTrace() {
        return initializationTrace;
    }

    private record ReadingTransaction(SingleDbTransaction singleDbTransaction, boolean readingOnly) {}

    public Set<InternalTransaction> getInternalTransactions() {
        return localTransactionContext.getInternalTransactions();
    }

    private enum State {
        OPEN,
        CLOSED,
        TERMINATED
    }

    private record ErrorRecord(String message, Throwable error) {}
}
