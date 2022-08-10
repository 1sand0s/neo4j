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
package org.neo4j.commandline.dbms;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import org.eclipse.collections.impl.set.mutable.MutableSetFactoryImpl;
import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.Util;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.dbms.archive.DumpFormatSelector;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Parameters;

@Command(
        name = "dump",
        header = "Dump a database into a single-file archive.",
        description = "Dump a database into a single-file archive. The archive can be used by the load command. "
                + "<destination-path> should be a directory (in which case a file called <database>.dump will "
                + "be created), or --to-stdout can be supplied to use standard output. "
                + "It is not possible to dump a database that is mounted in a running Neo4j server.")
public class DumpCommand extends AbstractCommand {
    @Parameters(
            arity = "1",
            description = "Name of the database to dump. Can contain * and ? for globbing.",
            converter = Converters.DatabaseNamePatternConverter.class)
    private DatabaseNamePattern database;

    @ArgGroup(multiplicity = "1")
    private TargetOption target = new TargetOption();

    private static class TargetOption {
        @Option(names = "--to-path", paramLabel = "<path>", description = "Destination folder of database dump.")
        private String toDir;

        @Option(names = "--to-stdout", description = "Use standard output as destination for database dump.")
        private boolean toStdout;
    }

    private final Dumper dumper;

    public DumpCommand(ExecutionContext ctx, Dumper dumper) {
        super(ctx);
        this.dumper = requireNonNull(dumper);
    }

    @Override
    public void execute() {
        if (target.toDir != null && !Files.isDirectory(Path.of(target.toDir))) {
            throw new CommandFailedException(target.toDir + " is not an existing directory");
        }

        boolean toStdOut = target.toStdout;
        if (toStdOut && database.containsPattern()) {
            throw new CommandFailedException("Globbing in database name can not be used in combination with standard "
                    + "output. Specify a directory as destination or a single target database");
        }

        Config config = CommandHelpers.buildConfig(ctx, allowCommandExpansion);
        InternalLog log;
        try (Log4jLogProvider logProvider = Util.configuredLogProvider(config, ctx.out())) {
            log = logProvider.getLog(getClass());
            Set<String> dbNames;
            List<FailedDump> failedDumps = new ArrayList<>();

            try (DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
                dbNames = getDbNames(config, fs);

                var memoryTracker = EmptyMemoryTracker.INSTANCE;

                for (String databaseName : dbNames) {
                    try {
                        if (!toStdOut) {
                            log.info(format("Starting dump of database '%s'", databaseName));
                        }

                        DatabaseLayout databaseLayout = Neo4jLayout.of(config).databaseLayout(databaseName);

                        try {
                            Validators.CONTAINS_EXISTING_DATABASE.validate(databaseLayout.databaseDirectory());
                        } catch (IllegalArgumentException e) {
                            throw new CommandFailedException("Database does not exist: " + databaseName, e);
                        }

                        if (fs.fileExists(databaseLayout.file(StoreMigrator.MIGRATION_DIRECTORY))) {
                            throw new CommandFailedException(
                                    "Store migration folder detected - A dump can not be taken during a store migration. Make sure "
                                            + "store migration is completed before trying again.");
                        }

                        try (Closeable ignored = LockChecker.checkDatabaseLock(databaseLayout)) {
                            checkDbState(ctx.fs(), databaseLayout, config, memoryTracker, databaseName);
                            dump(databaseLayout, databaseName);
                        } catch (FileLockException e) {
                            throw new CommandFailedException(
                                    "The database is in use. Stop database '" + databaseName + "' and try again.", e);
                        } catch (IOException e) {
                            wrapIOException(e);
                        } catch (CannotWriteException e) {
                            throw new CommandFailedException("You do not have permission to dump the database.", e);
                        }

                    } catch (Exception e) {
                        log.error("Failed to dump database '" + databaseName + "': " + e.getMessage());
                        failedDumps.add(new FailedDump(databaseName, e));
                    }
                }
            } catch (IOException e) {
                wrapIOException(e);
            }

            if (failedDumps.isEmpty()) {
                if (!toStdOut) {
                    log.info("Dump completed successfully");
                }
            } else {
                StringJoiner failedDbs = new StringJoiner("', '", "Dump failed for databases: '", "'");
                Exception exceptions = null;
                for (FailedDump failedDump : failedDumps) {
                    failedDbs.add(failedDump.dbName);
                    exceptions = Exceptions.chain(exceptions, failedDump.e);
                }
                log.error(failedDbs.toString());
                throw new CommandFailedException(failedDbs.toString(), exceptions);
            }
        }
    }

    record FailedDump(String dbName, Exception e) {}

    private Set<String> getDbNames(Config config, FileSystemAbstraction fs) {
        if (!database.containsPattern()) {
            return Set.of(database.getDatabaseName());
        } else {
            Set<String> dbNames = MutableSetFactoryImpl.INSTANCE.empty();
            Path databasesDir = Neo4jLayout.of(config).databasesDirectory();
            try {
                for (Path path : fs.listFiles(databasesDir)) {
                    if (fs.isDirectory(path)) {
                        String name = path.getFileName().toString();
                        if (database.matches(name)) {
                            dbNames.add(name);
                        }
                    }
                }
            } catch (IOException e) {
                throw new CommandFailedException("Failed to list databases", e);
            }
            return dbNames;
        }
    }

    private static Path buildArchivePath(String database, Path to) {
        return Files.isDirectory(to) ? to.resolve(database + ".dump") : to;
    }

    private OutputStream openDumpStream(String databaseName, TargetOption destination) throws IOException {
        if (destination.toStdout) {
            return ctx.out();
        }
        var archive = buildArchivePath(databaseName, Path.of(destination.toDir).toAbsolutePath());
        return dumper.openForDump(archive);
    }

    private void dump(DatabaseLayout databaseLayout, String databaseName) {
        Path databasePath = databaseLayout.databaseDirectory();
        try {
            var format = DumpFormatSelector.selectFormat(ctx.err());
            var lockFile = databaseLayout.databaseLockFile().getFileName().toString();
            var quarantineMarkerFile =
                    databaseLayout.quarantineMarkerFile().getFileName().toString();
            var out = openDumpStream(databaseName, target);
            dumper.dump(
                    databasePath,
                    databaseLayout.getTransactionLogsDirectory(),
                    out,
                    format,
                    path -> oneOf(path, lockFile, quarantineMarkerFile));
        } catch (FileAlreadyExistsException e) {
            throw new CommandFailedException("Archive already exists: " + e.getMessage(), e);
        } catch (NoSuchFileException e) {
            if (Paths.get(e.getMessage()).toAbsolutePath().equals(databasePath)) {
                throw new CommandFailedException("Database does not exist: " + databaseLayout.getDatabaseName(), e);
            }
            wrapIOException(e);
        } catch (IOException e) {
            wrapIOException(e);
        }
    }

    private static boolean oneOf(Path path, String... names) {
        return ArrayUtil.contains(names, path.getFileName().toString());
    }

    protected void checkDbState(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config additionalConfiguration,
            MemoryTracker memoryTracker,
            String databaseName) {
        if (checkRecoveryState(fs, databaseLayout, additionalConfiguration, memoryTracker)) {
            throw new CommandFailedException(joinAsLines(
                    "Active logical log detected, this might be a source of inconsistencies.",
                    "Please recover database before running the dump.",
                    "To perform recovery please start database and perform clean shutdown."));
        }
    }

    private static boolean checkRecoveryState(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config additionalConfiguration,
            MemoryTracker memoryTracker) {
        try {
            return isRecoveryRequired(fs, databaseLayout, additionalConfiguration, memoryTracker);
        } catch (Exception e) {
            throw new CommandFailedException("Failure when checking for recovery state: '%s'." + e.getMessage(), e);
        }
    }

    private static void wrapIOException(IOException e) {
        throw new CommandFailedException(
                format("Unable to dump database: %s: %s", e.getClass().getSimpleName(), e.getMessage()), e);
    }
}
