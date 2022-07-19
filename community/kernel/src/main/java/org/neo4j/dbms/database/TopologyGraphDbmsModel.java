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
package org.neo4j.dbms.database;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.database.NamedDatabaseId;

public interface TopologyGraphDbmsModel {
    enum HostedOnMode {
        RAFT(1, "raft"),
        REPLICA(2, "replica"),
        SINGLE(0, "single");

        private final String modeName;
        private final byte code;

        HostedOnMode(int code, String modeName) {
            this.code = (byte) code;
            this.modeName = modeName;
        }

        public String modeName() {
            return modeName;
        }

        public byte code() {
            return code;
        }

        public static HostedOnMode from(String modeName) {
            requireNonNull(modeName);

            for (HostedOnMode mode : values()) {
                if (modeName.equals(mode.modeName)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Enum value not found for requested modeName: " + modeName);
        }

        public static HostedOnMode forCode(byte code) {
            return Arrays.stream(values())
                    .filter(value -> value.code == code)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Invalid hosted on mode: " + code));
        }
    }

    enum DatabaseStatus {
        ONLINE("online"),
        OFFLINE("offline");

        private final String statusName;

        DatabaseStatus(String statusName) {
            this.statusName = statusName;
        }

        public String statusName() {
            return statusName;
        }
    }

    enum DatabaseAccess {
        READ_ONLY("read-only"),
        READ_WRITE("read-write");

        private final String stringRepr;

        DatabaseAccess(String stringRepr) {
            this.stringRepr = stringRepr;
        }

        public String getStringRepr() {
            return stringRepr;
        }
    }

    enum InstanceStatus {
        ENABLED,
        DEALLOCATING,
        MARKED;

        public static InstanceStatus getInstanceStatus(String value) {
            if (value.equals("active")) {
                return InstanceStatus.ENABLED;
            } else if (value.equals("draining")) {
                return InstanceStatus.DEALLOCATING;
            }
            return InstanceStatus.valueOf(value);
        }
    }

    Label DATABASE_LABEL = Label.label("Database");
    String DATABASE = DATABASE_LABEL.name();
    Label DELETED_DATABASE_LABEL = Label.label("DeletedDatabase");
    String DATABASE_UUID_PROPERTY = "uuid";
    String DATABASE_NAME_PROPERTY = "name";
    String DATABASE_STATUS_PROPERTY = "status";
    String DATABASE_ACCESS_PROPERTY = "access";
    String DATABASE_DEFAULT_PROPERTY = "default";
    String DATABASE_UPDATE_ID_PROPERTY = "update_id";
    String DATABASE_STORE_RANDOM_ID_PROPERTY = "store_random_id";
    String DATABASE_DESIGNATED_SEEDER_PROPERTY = "designated_seeder";
    String DATABASE_STORE_FORMAT_NEW_DB_PROPERTY = "creation_store_format";
    String DATABASE_PRIMARIES_PROPERTY = "primaries";
    String DATABASE_SECONDARIES_PROPERTY = "secondaries";
    String DATABASE_CREATED_AT_PROPERTY = "created_at";
    String DATABASE_STARTED_AT_PROPERTY = "started_at";
    String DATABASE_UPDATED_AT_PROPERTY = "updated_at";
    String DATABASE_STOPPED_AT_PROPERTY = "stopped_at";
    String DELETED_DATABASE_DUMP_DATA_PROPERTY = "dump_data";
    String DELETED_DATABASE_DELETED_AT_PROPERTY = "deleted_at";

    @Deprecated
    String DATABASE_INITIAL_SERVERS_PROPERTY = "initial_members";

    @Deprecated
    String DATABASE_STORE_CREATION_TIME_PROPERTY = "store_creation_time";

    @Deprecated
    String DATABASE_STORE_VERSION_PROPERTY = "store_version";

    Label DATABASE_NAME_LABEL = Label.label("DatabaseName");
    String DATABASE_NAME = DATABASE_NAME_LABEL.name();
    String DATABASE_NAME_LABEL_DESCRIPTION = "Database alias";
    String NAME_PROPERTY = "name";
    String VERSION_PROPERTY = "version"; // used to refresh connection pool on change
    RelationshipType TARGETS_RELATIONSHIP = RelationshipType.withName("TARGETS");
    String TARGETS = TARGETS_RELATIONSHIP.name();
    String TARGET_NAME_PROPERTY = "target_name";
    String PRIMARY_PROPERTY = "primary";
    Label REMOTE_DATABASE_LABEL = Label.label("Remote");
    String REMOTE_DATABASE = REMOTE_DATABASE_LABEL.name();
    String URL_PROPERTY = "url";
    String USERNAME_PROPERTY = "username";
    String PASSWORD_PROPERTY = "password";
    String IV_PROPERTY = "iv"; // Initialization Vector for AES encryption
    Label DRIVER_SETTINGS_LABEL = Label.label("DriverSettings");
    String DRIVER_SETTINGS = DRIVER_SETTINGS_LABEL.name();
    String SSL_ENFORCED = "ssl_enforced";
    String CONNECTION_TIMEOUT = "connection_timeout";
    String CONNECTION_MAX_LIFETIME = "connection_max_lifetime";
    String CONNECTION_POOL_AQUISITION_TIMEOUT = "connection_pool_acquisition_timeout";
    String CONNECTION_POOL_IDLE_TEST = "connection_pool_idle_test";
    String CONNECTION_POOL_MAX_SIZE = "connection_pool_max_size";
    String LOGGING_LEVEL = "logging_level";

    RelationshipType CONNECTS_WITH_RELATIONSHIP = RelationshipType.withName("CONNECTS_WITH");
    String CONNECTS_WITH = CONNECTS_WITH_RELATIONSHIP.name();

    Label INSTANCE_LABEL = Label.label("Instance");
    Label REMOVED_INSTANCE_LABEL = Label.label("RemovedInstance");
    String INSTANCE_UUID_PROPERTY = "uuid";
    String INSTANCE_NAME_PROPERTY = "name";
    String INSTANCE_STATUS_PROPERTY = "status";
    String INSTANCE_DISCOVERED_AT_PROPERTY = "discovered_at";

    @Deprecated
    String INSTANCE_MODE_PROPERTY = "mode";

    String INSTANCE_ALLOWED_DATABASES_PROPERTY = "allowedDatabases";
    String INSTANCE_DENIED_DATABASES_PROPERTY = "deniedDatabases";
    String INSTANCE_MODE_CONSTRAINT_PROPERTY = "modeConstraint";
    String REMOVED_INSTANCE_REMOVED_AT_PROPERTY = "removed_at";
    String REMOVED_INSTANCE_ALIASES_PROPERTY = "aliases";

    RelationshipType HOSTED_ON_RELATIONSHIP = RelationshipType.withName("HOSTED_ON");
    RelationshipType WAS_HOSTED_ON_RELATIONSHIP = RelationshipType.withName("WAS_HOSTED_ON");
    String HOSTED_ON_INSTALLED_AT_PROPERTY = "installed_at";
    String HOSTED_ON_BOOTSTRAPPER_PROPERTY = "bootstrapper";
    String HOSTED_ON_MODE_PROPERTY = "mode";
    String WAS_HOSTED_ON_REMOVED_AT_PROPERTY = "removed_at";

    Label TOPOLOGY_GRAPH_SETTINGS_LABEL = Label.label("TopologyGraphSettings");
    String TOPOLOGY_GRAPH_SETTINGS_ALLOCATOR_PROPERTY = "allocator";

    Set<NamedDatabaseId> getAllDatabaseIds();

    /**
     * Fetches the {@link NamedDatabaseId} corresponding to the provided alias, if one exists in this DBMS.
     * <p>
     * Note: The returned id will have its *true* name (primary alias), rather than the provided databaseName, which may be an (secondary) alias.
     *
     * @param databaseName the database alias to resolve a {@link NamedDatabaseId} for.
     * @return the corresponding {@link NamedDatabaseId}
     */
    Optional<NamedDatabaseId> getDatabaseIdByAlias(String databaseName);

    /**
     * Fetches the {@link NamedDatabaseId} corresponding to the provided id, if one exists in this DBMS.
     *
     * @param uuid the uuid to resolve a {@link NamedDatabaseId} for.
     * @return the corresponding {@link NamedDatabaseId}
     */
    Optional<NamedDatabaseId> getDatabaseIdByUUID(UUID uuid);

    /**
     * Fetches all known database aliases in the DBMS, along with their associated {@link NamedDatabaseId}s.
     *
     * @return the aliases and associated ids
     */
    Map<String, NamedDatabaseId> getAllDatabaseAliases();
}
