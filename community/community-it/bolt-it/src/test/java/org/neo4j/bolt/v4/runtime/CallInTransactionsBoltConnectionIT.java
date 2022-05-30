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
package org.neo4j.bolt.v4.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.bolt.testing.assertions.ResponseRecorderAssertions.assertThat;
import static org.neo4j.bolt.testing.messages.BoltV40Messages.run;
import static org.neo4j.bolt.testing.messages.BoltV44Messages.pull;
import static org.neo4j.bolt.v4.runtime.BoltConnectionIT.IRIS_DATA;
import static org.neo4j.bolt.v4.runtime.BoltConnectionIT.createLocalIrisData;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.values.storable.Values.longValue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.bolt.runtime.SessionExtension;
import org.neo4j.bolt.testing.response.ResponseRecorder;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.ValueUtils;

class CallInTransactionsBoltConnectionIT {
    @RegisterExtension
    static final SessionExtension env = new SessionExtension();

    @Test
    void shouldSupportUsingCallInTransactionsInSession() throws Exception {
        // Given
        var machine = BoltStateMachineV4StateTestBase.newStateMachineAfterAuth(env);
        var params = ValueUtils.asMapValue(MapUtil.map("csvFileUrl", createLocalIrisData(machine)));
        var txIdBeforeQuery = env.lastClosedTxId();
        var batch = 40;

        // When
        var recorder = new ResponseRecorder();
        machine.process(
                run(
                        joinAsLines(
                                "LOAD CSV WITH HEADERS FROM $csvFileUrl AS l",
                                "CALL {",
                                "  WITH l",
                                "  MATCH (c:Class {name: l.class_name})",
                                "  CREATE (s:Sample {sepal_length: l.sepal_length,",
                                "                    sepal_width: l.sepal_width,",
                                "                    petal_length: l.petal_length,",
                                "                    petal_width: l.petal_width})",
                                "  CREATE (c)<-[:HAS_CLASS]-(s)",
                                "  RETURN c, s",
                                "} IN TRANSACTIONS OF 40 ROWS",
                                "RETURN count(*) AS c"),
                        params),
                recorder);
        machine.process(pull(), recorder);

        // Then
        assertThat(recorder).hasSuccessResponse().hasRecord(longValue(150L)).hasSuccessResponse();

        /*
         * 7 tokens have been created for
         * 'Sample' label
         * 'HAS_CLASS' relationship type
         * 'name', 'sepal_length', 'sepal_width', 'petal_length', and 'petal_width' property keys
         *
         * Note that the token id for the label 'Class' has been created in `createLocalIrisData(...)` so it shouldn't1
         * be counted again here
         */
        var tokensCommits = 7;
        var commits = (IRIS_DATA.split("\n").length - 1 /* header */) / batch;
        var txId = env.lastClosedTxId();
        assertEquals(tokensCommits + commits + txIdBeforeQuery, txId);
    }
}
