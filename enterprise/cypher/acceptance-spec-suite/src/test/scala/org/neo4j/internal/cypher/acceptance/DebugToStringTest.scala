/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.util.v3_4.test_helpers.WindowsStringSafe


class DebugToStringTest extends ExecutionEngineFunSuite {

  implicit val windowsSafe = WindowsStringSafe

  /**
    * This tests an internal feature that is not supported or critical for end users. Still nice to see that it works
    * and what the expected outputs are.
    */

  test("ast-tostring works") {
    val textResult = graph.execute(queryWithOutputOf("ast")).resultAsString()

    textResult should equal("""+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                              || col                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
                              |+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                              || "Query(None,SingleQuery(List(Match(false,Pattern(List(EveryPath(RelationshipChain(NodePattern(Some(Variable(a)),List(),None),RelationshipPattern(Some(Variable(  UNNAMED10)),List(RelTypeName(T)),None,None,OUTGOING,false),NodePattern(Some(Variable(b)),List(),None))))),List(),None), Return(false,ReturnItems(false,Vector(AliasedReturnItem(Variable(a),Variable(a)), AliasedReturnItem(Variable(b),Variable(b)))),None,None,None,None,Set()))))" |
                              |+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                              |1 row
                              |""".stripMargin)
  }

  test("logicalplan-tostring works") {
    val textResult = graph.execute(queryWithOutputOf("logicalPlan")).resultAsString()

    textResult should equal( """+-----------------------------------------------------------------------------------+
                               || col                                                                               |
                               |+-----------------------------------------------------------------------------------+
                               || "ProduceResult(Vector(a, b)) {"                                                   |
                               || "  LHS -> Expand(a, OUTGOING, List(RelTypeName(T)), b,   UNNAMED10, ExpandAll) {" |
                               || "    LHS -> AllNodesScan(a, Set()) {}"                                            |
                               || "  }"                                                                             |
                               || "}"                                                                               |
                               |+-----------------------------------------------------------------------------------+
                               |5 rows
                               |""".stripMargin)
  }

  test("queryGraph-tostring works") {
    val textResult = graph.execute(queryWithOutputOf("queryGraph")).resultAsString()

    textResult should equal("""+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                              || col                                                                                                                                                                                                                                       |
                              |+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                              || "UnionQuery(List(RegularPlannerQuery(QueryGraph {Nodes: ['a', 'b'], Rels: ['(a)--[  UNNAMED10:T]->-(b)']},RegularQueryProjection(Map(a -> Variable(a), b -> Variable(b)),QueryShuffle(List(),None,None)),None)),false,Vector(a, b),None)" |
                              |+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                              |1 row
                              |""".stripMargin)
  }

  test("semanticState-tostring works") {
    val textResult = graph.execute(queryWithOutputOf("semanticState")).resultAsString()

    // There's no good toString for SemanticState, which makes it difficult to test the output in a good way
    textResult should include("ScopeLocation")
  }

  test("cost reporting") {
    val stringResult = graph.execute("CYPHER debug=dumpCosts MATCH (a:A) RETURN *").resultAsString()
    stringResult should equal("""+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                                || # | planId | planText                                                             | planCost                                                          | cost   | est cardinality | winner |
                                |+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                                || 1 | 0      | ""                                                                   | ""                                                                | 10.0   | 1.0             | "WON"  |
                                || 1 | <null> | "NodeByLabelScan(a, LabelName(A), Set()) {}"                         | "NodeByLabelScan costs Cost(10.0) cardinality Cardinality(1.0)"   | <null> | <null>          | <null> |
                                || 1 | 2      | ""                                                                   | ""                                                                | 13.0   | 1.0             | "LOST" |
                                || 1 | <null> | "Selection(ListBuffer(HasLabels(Variable(a),List(LabelName(A))))) {" | "Selection costs Cost(13.0) cardinality Cardinality(1.0)"         | <null> | <null>          | <null> |
                                || 1 | <null> | "  LHS -> AllNodesScan(a, Set()) {}"                                 | "  AllNodesScan costs Cost(12.0) cardinality Cardinality(1.0)"    | <null> | <null>          | <null> |
                                || 0 | 2      | ""                                                                   | ""                                                                | 13.0   | 1.0             | "WON"  |
                                || 0 | <null> | "Selection(ListBuffer(HasLabels(Variable(a),List(LabelName(A))))) {" | "Selection costs Cost(13.0) cardinality Cardinality(1.0)"         | <null> | <null>          | <null> |
                                || 0 | <null> | "  LHS -> AllNodesScan(a, Set()) {}"                                 | "  AllNodesScan costs Cost(12.0) cardinality Cardinality(1.0)"    | <null> | <null>          | <null> |
                                || 0 | 4      | ""                                                                   | ""                                                                | 27.5   | 1.0             | "LOST" |
                                || 0 | <null> | "NodeHashJoin(Set(a)) {"                                             | "NodeHashJoin costs Cost(27.5) cardinality Cardinality(1.0)"      | <null> | <null>          | <null> |
                                || 0 | <null> | "  LHS -> AllNodesScan(a, Set()) {}"                                 | "  AllNodesScan costs Cost(12.0) cardinality Cardinality(1.0)"    | <null> | <null>          | <null> |
                                || 0 | <null> | "  RHS -> NodeByLabelScan(a, LabelName(A), Set()) {}"                | "  NodeByLabelScan costs Cost(10.0) cardinality Cardinality(1.0)" | <null> | <null>          | <null> |
                                |+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                                |12 rows
                                |""".stripMargin)
    graph.execute("CYPHER debug=dumpCosts MATCH (a:A:B:C)-[:T]->(b:A:B:C) RETURN *").stream().count()
  }

  private def queryWithOutputOf(s: String) = s"CYPHER debug=tostring debug=$s MATCH (a)-[:T]->(b) RETURN *"
}
