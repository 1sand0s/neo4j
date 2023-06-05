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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.parser.CypherParser

class ProcedureCallParserTest extends ParserSyntaxTreeBase[Cst.CallClause, Clause] {

  implicit private val javaccRule: JavaccRule[Clause] = JavaccRule.CallClause
  implicit private val antlrRule: AntlrRule[CypherParser.CallClauseContext] = AntlrRule.CallClause

  test("CALL foo") {
    gives(call(Seq.empty, "foo", None))
  }

  test("CALL foo()") {
    gives(call(Seq.empty, "foo", Some(Seq.empty)))
  }

  test("CALL foo('Test', 1+2)") {
    gives(call(Seq.empty, "foo", Some(Vector(literalString("Test"), add(literalInt(1), literalInt(2))))))
  }

  test("CALL foo.bar.baz('Test', 1+2)") {
    gives(call(List("foo", "bar"), "baz", Some(Vector(literalString("Test"), add(literalInt(1), literalInt(2))))))
  }

  test("CALL foo YIELD bar") {
    gives(call(Seq.empty, "foo", None, Some(Seq(varFor("bar")))))
  }

  test("CALL foo YIELD bar, baz") {
    gives(call(Seq.empty, "foo", None, Some(Seq(varFor("bar"), varFor("baz")))))
  }

  test("CALL foo() YIELD bar") {
    gives(call(Seq.empty, "foo", Some(Seq.empty), Some(Seq(varFor("bar")))))
  }

  test("CALL foo() YIELD bar, baz") {
    gives(call(Seq.empty, "foo", Some(Seq.empty), Some(Seq(varFor("bar"), varFor("baz")))))
  }
}
