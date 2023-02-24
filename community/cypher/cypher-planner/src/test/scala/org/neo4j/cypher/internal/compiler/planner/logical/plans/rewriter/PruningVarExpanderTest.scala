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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PruningVarExpanderTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("simplest possible query that can use PruningVarExpand") {
    // Simplest query:
    // match (a)-[*2..3]->(b) return distinct b

    val fromId = "from"
    val allNodes = AllNodesScan(fromId, Set.empty)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(2, Some(3))
    val toId = "to"
    val relId = "r"
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll)
    val input = Distinct(originalExpand, Map("to" -> varFor("to")))

    val rewrittenExpand = PruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, 2, 3)
    val expectedOutput = Distinct(rewrittenExpand, Map("to" -> varFor("to")))

    rewrite(input) should equal(expectedOutput)
  }

  test("simplest possible query that can use BFSPruningVarExpand") {
    // Simplest query:
    // match (a)-[*1..3]->(b) return distinct b

    val fromId = "from"
    val allNodes = AllNodesScan(fromId, Set.empty)
    val dir = SemanticDirection.OUTGOING
    val length = VarPatternLength(1, Some(3))
    val toId = "to"
    val relId = "r"
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll)
    val input = Distinct(originalExpand, Map("to" -> varFor("to")))

    val rewrittenExpand = BFSPruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, includeStartNode = false, 3)
    val expectedOutput = Distinct(rewrittenExpand, Map("to" -> varFor("to")))

    rewrite(input) should equal(expectedOutput)
  }

  test("do use BFSPruningVarExpand for undirected search") {
    val fromId = "from"
    val allNodes = AllNodesScan(fromId, Set.empty)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))
    val toId = "to"
    val relId = "r"
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll)
    val input = Distinct(originalExpand, Map("to" -> varFor("to")))

    val rewrittenExpand = BFSPruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, includeStartNode = false, 3)
    val expectedOutput = Distinct(rewrittenExpand, Map("to" -> varFor("to")))

    rewrite(input) should equal(expectedOutput)
  }

  test("ordered distinct with pruningVarExpand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("a"), "a AS a")
      .expandAll("(a)-[*2..3]->(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("a"), "a AS a")
      .pruningVarExpand("(a)-[*2..3]->(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("ordered distinct with BFSPruningVarExpand") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("a"), "a AS a")
      .expandAll("(a)-[*1..3]->(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .orderedDistinct(Seq("a"), "a AS a")
      .bfsPruningVarExpand("(a)-[*1..3]->(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("query with distinct aggregation") {
    // Simplest query:
    // match (from)-[2..3]->(to) return count(distinct to)

    val fromId = "from"
    val allNodes = AllNodesScan(fromId, Set.empty)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(2, Some(3))
    val toId = "to"
    val relId = "r"
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll)
    val aggregatingExpression = distinctFunction("count", varFor("to"))
    val input = Aggregation(originalExpand, Map.empty, Map("x" -> aggregatingExpression))

    val rewrittenExpand = PruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, 2, 3)
    val expectedOutput = Aggregation(rewrittenExpand, Map.empty, Map("x" -> aggregatingExpression))

    rewrite(input) should equal(expectedOutput)
  }

  test("query with distinct aggregation and BFSPruningVarExpand") {
    // Simplest query:
    // match (from)-[1..3]->(to) return count(distinct to)

    val fromId = "from"
    val allNodes = AllNodesScan(fromId, Set.empty)
    val dir = SemanticDirection.INCOMING
    val length = VarPatternLength(1, Some(3))
    val toId = "to"
    val relId = "r"
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll)
    val aggregatingExpression = distinctFunction("count", varFor("to"))
    val input = Aggregation(originalExpand, Map.empty, Map("x" -> aggregatingExpression))

    val rewrittenExpand = BFSPruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, includeStartNode = false, 3)
    val expectedOutput = Aggregation(rewrittenExpand, Map.empty, Map("x" -> aggregatingExpression))

    rewrite(input) should equal(expectedOutput)
  }

  test("ordered grouping aggregation") {
    val before = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("a AS a"), Seq("count(distinct b) AS c"), Seq("a"))
      .expand("(a)-[*2..3]->(b)")
      .allNodeScan("a")
      .build()

    val after = new LogicalPlanBuilder(wholePlan = false)
      .orderedAggregation(Seq("a AS a"), Seq("count(distinct b) AS c"), Seq("a"))
      .pruningVarExpand("(a)-[*2..3]->(b)")
      .allNodeScan("a")
      .build()

    rewrite(before) should equal(after)
  }

  test("Simple query that filters between expand and distinct") {
    // Simplest query:
    // match (a)-[*2..3]->(b:X) return distinct b

    val fromId = "from"
    val allNodes = AllNodesScan(fromId, Set.empty)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(2, Some(3))
    val toId = "to"
    val relId = "r"
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll)
    val predicate = hasLabels("to", "X")
    val filter = Selection(Seq(predicate), originalExpand)
    val input = Distinct(filter, Map("to" -> varFor("to")))

    val rewrittenExpand = PruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, 2, 3)
    val filterAfterRewrite = Selection(Seq(predicate), rewrittenExpand)
    val expectedOutput = Distinct(filterAfterRewrite, Map("to" -> varFor("to")))

    rewrite(input) should equal(expectedOutput)
  }

  test("Simple query that filters between expand and distinct and BFSPruningVarExpand") {
    // Simplest query:
    // match (a)-[*1..3]->(b:X) return distinct b

    val fromId = "from"
    val allNodes = AllNodesScan(fromId, Set.empty)
    val dir = SemanticDirection.OUTGOING
    val length = VarPatternLength(0, Some(3))
    val toId = "to"
    val relId = "r"
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll)
    val predicate = hasLabels("to", "X")
    val filter = Selection(Seq(predicate), originalExpand)
    val input = Distinct(filter, Map("to" -> varFor("to")))

    val rewrittenExpand = BFSPruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, includeStartNode = true, 3)
    val filterAfterRewrite = Selection(Seq(predicate), rewrittenExpand)
    val expectedOutput = Distinct(filterAfterRewrite, Map("to" -> varFor("to")))

    rewrite(input) should equal(expectedOutput)
  }

  test("Query that aggregates before making the result DISTINCT") {
    // Simplest query:
    // match (a)-[:R*1..3]->(b) with count(*) as count return distinct count

    val fromId = "from"
    val allNodes = AllNodesScan(fromId, Set.empty)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))
    val toId = "to"
    val relId = "r"
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll)
    val aggregate = Aggregation(originalExpand, Map.empty, Map("count" -> CountStar()(pos)))
    val input = Distinct(aggregate, Map("to" -> varFor("to")))

    assertNotRewritten(input)
  }

  test("Double var expand with distinct result") {
    // Simplest query:
    // match (a)-[:R*2..3]->(b)-[:T*2..3]->(c) return distinct c

    val aId = "a"
    val relRId = "r"
    val bId = "b"
    val relTId = "t"
    val cId = "c"
    val allNodes = AllNodesScan(aId, Set.empty)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(2, Some(3))
    val originalExpand = VarExpand(allNodes, aId, dir, dir, Seq(RelTypeName("R")(pos)), bId, relRId, length, ExpandAll)
    val originalExpand2 =
      VarExpand(originalExpand, bId, dir, dir, Seq(RelTypeName("T")(pos)), cId, relTId, length, ExpandAll)
    val input = Distinct(originalExpand2, Map("c" -> varFor("c")))

    val rewrittenExpand = PruningVarExpand(allNodes, aId, dir, Seq(RelTypeName("R")(pos)), bId, 2, 3)
    val rewrittenExpand2 = PruningVarExpand(rewrittenExpand, bId, dir, Seq(RelTypeName("T")(pos)), cId, 2, 3)
    val expectedOutput = Distinct(rewrittenExpand2, Map("c" -> varFor("c")))

    rewrite(input) should equal(expectedOutput)
  }

  test("Double var expand with distinct result with BFSPruningVarExpand") {
    // Simplest query:
    // match (a)-[:R*1..3]->(b)-[:T*1..3]->(c) return distinct c

    val aId = "a"
    val relRId = "r"
    val bId = "b"
    val relTId = "t"
    val cId = "c"
    val allNodes = AllNodesScan(aId, Set.empty)
    val dir = SemanticDirection.OUTGOING
    val length = VarPatternLength(1, Some(3))
    val originalExpand = VarExpand(allNodes, aId, dir, dir, Seq(RelTypeName("R")(pos)), bId, relRId, length, ExpandAll)
    val originalExpand2 =
      VarExpand(originalExpand, bId, dir, dir, Seq(RelTypeName("T")(pos)), cId, relTId, length, ExpandAll)
    val input = Distinct(originalExpand2, Map("c" -> varFor("c")))

    val rewrittenExpand =
      BFSPruningVarExpand(allNodes, aId, dir, Seq(RelTypeName("R")(pos)), bId, includeStartNode = false, 3)
    val rewrittenExpand2 =
      BFSPruningVarExpand(rewrittenExpand, bId, dir, Seq(RelTypeName("T")(pos)), cId, includeStartNode = false, 3)
    val expectedOutput = Distinct(rewrittenExpand2, Map("c" -> varFor("c")))

    rewrite(input) should equal(expectedOutput)
  }

  test("var expand followed by normal expand") {
    // Simplest query:
    // match (a)-[:R*2..3]->(b)-[:T]->(c) return distinct c

    val aId = "a"
    val relRId = "r"
    val bId = "b"
    val relTId = "t"
    val cId = "c"
    val allNodes = AllNodesScan(aId, Set.empty)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(2, Some(3))
    val originalExpand = VarExpand(allNodes, aId, dir, dir, Seq(RelTypeName("R")(pos)), bId, relRId, length, ExpandAll)
    val originalExpand2 = Expand(originalExpand, bId, dir, Seq(RelTypeName("T")(pos)), cId, relTId)
    val input = Distinct(originalExpand2, Map("c" -> varFor("c")))

    val rewrittenExpand = PruningVarExpand(allNodes, aId, dir, Seq(RelTypeName("R")(pos)), bId, 2, 3)
    val rewrittenExpand2 = Expand(rewrittenExpand, bId, dir, Seq(RelTypeName("T")(pos)), cId, relTId)
    val expectedOutput = Distinct(rewrittenExpand2, Map("c" -> varFor("c")))

    rewrite(input) should equal(expectedOutput)
  }

  test("var expand followed by normal expand with BFSPruningVarExpand") {
    // Simplest query:
    // match (a)-[:R*..3]->(b)-[:T]->(c) return distinct c

    val aId = "a"
    val relRId = "r"
    val bId = "b"
    val relTId = "t"
    val cId = "c"
    val allNodes = AllNodesScan(aId, Set.empty)
    val dir = SemanticDirection.INCOMING
    val length = VarPatternLength(1, Some(3))
    val originalExpand = VarExpand(allNodes, aId, dir, dir, Seq(RelTypeName("R")(pos)), bId, relRId, length, ExpandAll)
    val originalExpand2 = Expand(originalExpand, bId, dir, Seq(RelTypeName("T")(pos)), cId, relTId)
    val input = Distinct(originalExpand2, Map("c" -> varFor("c")))

    val rewrittenExpand =
      BFSPruningVarExpand(allNodes, aId, dir, Seq(RelTypeName("R")(pos)), bId, includeStartNode = false, 3)
    val rewrittenExpand2 = Expand(rewrittenExpand, bId, dir, Seq(RelTypeName("T")(pos)), cId, relTId)
    val expectedOutput = Distinct(rewrittenExpand2, Map("c" -> varFor("c")))

    rewrite(input) should equal(expectedOutput)
  }

  test("optional match can be solved with PruningVarExpand") {
    /* Simplest query:
       match (a) optional match (a)-[:R*1..3]->(b)-[:T]->(c) return distinct c
       in logical plans:

              distinct1                            distinct2
                 |                                    |
               apply1                               apply2
               /   \                                /   \
       all-nodes   optional1      ===>       all-nodes  optional2
                     \                                    \
                     var-length-expand                   pruning-var-expand
                       \                                    \
                       argument                            argument
     */

    val aId = "a"
    val relRId = "r"
    val bId = "b"
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(2, Some(3))

    val allNodes = AllNodesScan(aId, Set.empty)
    val argument = Argument(Set(aId))
    val originalExpand = VarExpand(argument, aId, dir, dir, Seq(RelTypeName("R")(pos)), bId, relRId, length, ExpandAll)
    val optional1 = Optional(originalExpand, Set(aId))
    val apply1 = Apply(allNodes, optional1)
    val distinct1 = Distinct(apply1, Map("c" -> varFor("c")))

    val rewrittenExpand = PruningVarExpand(argument, aId, dir, Seq(RelTypeName("R")(pos)), bId, 2, 3)
    val optional2 = Optional(rewrittenExpand, Set(aId))
    val apply2 = Apply(allNodes, optional2)
    val distinct2 = Distinct(apply2, Map("c" -> varFor("c")))

    rewrite(distinct1) should equal(distinct2)
  }

  test("optional match can be solved with PruningVarExpand with BFSPruningVarExpand") {
    /* Simplest query:
       match (a) optional match (a)-[:R*1..3]->(b)-[:T]->(c) return distinct c
       in logical plans:

              distinct1                            distinct2
                 |                                    |
               apply1                               apply2
               /   \                                /   \
       all-nodes   optional1      ===>       all-nodes  optional2
                     \                                    \
                     var-length-expand               bfs-pruning-var-expand
                       \                                    \
                       argument                            argument
     */

    val aId = "a"
    val relRId = "r"
    val bId = "b"
    val dir = SemanticDirection.OUTGOING
    val length = VarPatternLength(1, Some(3))

    val allNodes = AllNodesScan(aId, Set.empty)
    val argument = Argument(Set(aId))
    val originalExpand = VarExpand(argument, aId, dir, dir, Seq(RelTypeName("R")(pos)), bId, relRId, length, ExpandAll)
    val optional1 = Optional(originalExpand, Set(aId))
    val apply1 = Apply(allNodes, optional1)
    val distinct1 = Distinct(apply1, Map("c" -> varFor("c")))

    val rewrittenExpand =
      BFSPruningVarExpand(argument, aId, dir, Seq(RelTypeName("R")(pos)), bId, includeStartNode = false, 3)
    val optional2 = Optional(rewrittenExpand, Set(aId))
    val apply2 = Apply(allNodes, optional2)
    val distinct2 = Distinct(apply2, Map("c" -> varFor("c")))

    rewrite(distinct1) should equal(distinct2)
  }

  test("should not rewrite when doing non-distinct aggregation") {
    // Should not be rewritten since it's asking for a count of all paths leading to a node
    // match (a)-[*1..3]->(b) return b, count(*)

    val fromId = "from"
    val allNodes = AllNodesScan(fromId, Set.empty)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))
    val toId = "to"
    val relId = "r"
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll)
    val input = Aggregation(originalExpand, Map("r" -> varFor("r")), Map.empty)

    assertNotRewritten(input)
  }

  test("on longer var-lengths, we also use PruningVarExpand") {
    // Simplest query:
    // match (a)-[*4..5]->(b) return distinct b

    val fromId = "from"
    val allNodes = AllNodesScan(fromId, Set.empty)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(4, Some(5))
    val toId = "to"
    val relId = "r"
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll)
    val input = Distinct(originalExpand, Map("to" -> varFor("to")))

    val rewrittenExpand = PruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, 4, 5)
    val expectedOutput = Distinct(rewrittenExpand, Map("to" -> varFor("to")))

    rewrite(input) should equal(expectedOutput)
  }

  test("do not use pruning for length=1") {
    // Simplest query:
    // match (a)-[*1..1]->(b) return distinct b

    val fromId = "from"
    val allNodes = AllNodesScan(fromId, Set.empty)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(1))
    val toId = "to"
    val relId = "r"
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll)
    val input = Distinct(originalExpand, Map("to" -> varFor("to")))

    assertNotRewritten(input)
  }

  test("do not use pruning for pathExpressions when path is needed") {
    // Simplest query:
    // match p=(from)-[r*0..1]->(to) with nodes(p) as d return distinct d

    val fromId = "from"
    val allNodes = AllNodesScan(fromId, Set.empty)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(0, Some(2))
    val toId = "to"

    val relId = "r"
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll)
    val pathProjector = NodePathStep(
      varFor("from"),
      MultiRelationshipPathStep(varFor("r"), SemanticDirection.BOTH, Some(varFor("to")), NilPathStep()(pos))(pos)
    )(pos)

    val func = function("nodes", PathExpression(pathProjector)(pos))
    val projection = Projection(originalExpand, Set.empty, Map("d" -> func))
    val distinct = Distinct(projection, Map("d" -> varFor("d")))

    assertNotRewritten(distinct)
  }

  test("do not use pruning-varexpand when both sides of the var-length-relationship are already known") {
    val fromId = "from"
    val toId = "to"
    val fromPlan = AllNodesScan(fromId, Set.empty)
    val toPlan = AllNodesScan(toId, Set.empty)
    val xJoin = CartesianProduct(fromPlan, toPlan)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))
    val relId = "r"
    val originalExpand = VarExpand(xJoin, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandInto)
    val input = Aggregation(originalExpand, Map("to" -> varFor("to")), Map.empty)

    assertNotRewritten(input)
  }

  test("should handle insanely long logical plans without running out of stack") {
    val leafPlan: LogicalPlan = Argument(Set("x"))
    var plan = leafPlan
    (1 until 10000) foreach { _ =>
      plan = Selection(Seq(trueLiteral), plan)
    }

    rewrite(plan) // should not throw exception
  }

  test("cartesian product can be solved with BFSPruningVarExpand") {
    /* Simplest query:
       match (a) match (b)-[:R*1..3]->(c) return distinct c
       in logical plans:

                               distinct
                                  |
                              cartesian
                                /   \
                 var-length-expand  all-nodes
                              /       \
                         all-nodes      arg
     */

    val aId = "a"
    val relRId = "r"
    val bId = "b"
    val cId = "c"
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))

    val original =
      Distinct(
        CartesianProduct(
          VarExpand(
            AllNodesScan(bId, Set.empty),
            bId,
            dir,
            dir,
            Seq(RelTypeName("R")(pos)),
            cId,
            relRId,
            length,
            ExpandAll
          ),
          AllNodesScan(aId, Set(bId, cId))
        ),
        Map(cId -> varFor(cId))
      )

    val rewritten =
      Distinct(
        CartesianProduct(
          BFSPruningVarExpand(
            AllNodesScan(bId, Set.empty),
            bId,
            dir,
            Seq(RelTypeName("R")(pos)),
            cId,
            includeStartNode = false,
            3
          ),
          AllNodesScan(aId, Set(bId, cId))
        ),
        Map(cId -> varFor(cId))
      )

    rewrite(original) should equal(rewritten)
  }

  test("do not use pruning-varexpand when upper bound < lower bound") {
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .distinct("a AS a")
      .expandAll("(a)-[r*3..2]->(b)")
      .allNodeScan("a")
      .build()

    assertNotRewritten(plan)
  }

  private def assertNotRewritten(p: LogicalPlan): Unit = {
    rewrite(p) should equal(p)
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    p.endoRewrite(pruningVarExpander)
}
