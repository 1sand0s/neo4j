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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.DeleteExpression
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.EagernessReason.LabelReadRemoveConflict
import org.neo4j.cypher.internal.ir.MergeNodePattern
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QgWithLeafInfo.qgWithNoStableIdentifierAndOnlyLeaves
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.removeLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setProperty
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.ListSet

class UpdateGraphTest extends CypherFunSuite with AstConstructionTestSupport {
  implicit private val semanticTable: SemanticTable = SemanticTable()

  test("should not be empty after adding label to set") {
    val original = QueryGraph()
    val setLabel = SetLabelPattern("name", Seq.empty)

    original.addMutatingPatterns(setLabel).containsUpdates should be(true)
  }

  test("overlap when reading all labels and creating a specific label") {
    // MATCH (a) CREATE (:L)
    val qg = QueryGraph(patternNodes = Set("a"))
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("no overlap when reading all labels and not setting any label") {
    // ... WITH a, labels(a) as myLabels SET a.prop=[]
    val qg = QueryGraph(
      argumentIds = Set("a"),
      selections =
        Selections(Set(Predicate(Set("a"), Variable("a")(pos)), Predicate(Set("a"), Labels(Variable("a")(pos))(pos))))
    )
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(setProperty("a", "prop", "[]")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe empty
  }

  test("overlap when reading all labels and removing a label") {
    // ... WITH a, labels(a) as myLabels REMOVE a:Label
    val qg = QueryGraph(
      argumentIds = Set("a"),
      selections =
        Selections(Set(Predicate(Set("a"), Variable("a")(pos)), Predicate(Set("a"), Labels(Variable("a")(pos))(pos))))
    )
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(removeLabel("a", "Label")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe ListSet(LabelReadRemoveConflict(labelName("Label")))
  }

  test("overlap when reading and creating the same label") {
    // MATCH (a:L) CREATE (b:L)
    val selections = Selections.from(HasLabels(Variable("a")(pos), Seq(LabelName("L")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set("a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("no overlap when reading and creating different labels") {
    // MATCH (a:L1:L2) CREATE (b:L3)
    val selections =
      Selections.from(HasLabels(Variable("a")(pos), Seq(LabelName("L1")(pos), LabelName("L2")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set("a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L3")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe empty
  }

  test("no overlap when properties don't overlap and no label on read GQ, but a label on write QG") {
    // MATCH (a {foo: 42}) CREATE (a:L)
    val selections =
      Selections.from(In(Variable("a")(pos), Property(Variable("a")(pos), PropertyKeyName("foo")(pos))(pos))(pos))
    val qg = QueryGraph(patternNodes = Set("a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe empty
  }

  test("overlap when properties don't overlap but labels explicitly do") {
    // MATCH (a:L {foo: 42}) CREATE (a:L) assuming `a` is unstable
    val selections = Selections.from(Seq(
      In(Variable("a")(pos), Property(Variable("a")(pos), PropertyKeyName("foo")(pos))(pos))(pos),
      HasLabels(Variable("a")(pos), Seq(LabelName("L")(pos)))(pos)
    ))
    val qg = QueryGraph(patternNodes = Set("a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createNode("b", "L")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("overlap when reading all rel types and creating a specific type") {
    // MATCH (a)-[r]->(b)  CREATE (a)-[r2:T]->(b)
    val qg = QueryGraph(patternRelationships =
      Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
    )
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createRelationship("r2", "a", "T", "b")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("no overlap when reading and writing different rel types") {
    // MATCH (a)-[r:T1]->(b)  CREATE (a)-[r2:T]->(b)
    val qg = QueryGraph(patternRelationships =
      Set(PatternRelationship(
        "r",
        ("a", "b"),
        SemanticDirection.OUTGOING,
        Seq(RelTypeName("T1")(pos)),
        SimplePatternLength
      ))
    )
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createRelationship("r2", "a", "T2", "b")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe empty
  }

  test("overlap when reading and writing same rel types") {
    // MATCH (a)-[r:T1]->(b)  CREATE (a)-[r2:T1]->(b)
    val qg = QueryGraph(patternRelationships =
      Set(PatternRelationship(
        "r",
        ("a", "b"),
        SemanticDirection.OUTGOING,
        Seq(RelTypeName("T1")(pos)),
        SimplePatternLength
      ))
    )
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createRelationship("r2", "a", "T1", "b")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("no overlap when reading and writing same rel types but matching on rel property") {
    // MATCH (a)-[r:T1 {foo: 42}]->(b)  CREATE (a)-[r2:T1]->(b)
    val selections =
      Selections.from(In(Variable("a")(pos), Property(Variable("r")(pos), PropertyKeyName("foo")(pos))(pos))(pos))
    val qg = QueryGraph(
      patternRelationships =
        Set(PatternRelationship(
          "r",
          ("a", "b"),
          SemanticDirection.OUTGOING,
          Seq(RelTypeName("T1")(pos)),
          SimplePatternLength
        )),
      selections = selections
    )
    val ug = QueryGraph(mutatingPatterns = IndexedSeq(createRelationship("r2", "a", "T1", "b")))

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe empty
  }

  test("overlap when reading and writing same property and rel type") {
    // MATCH (a)-[r:T1 {foo: 42}]->(b)  CREATE (a)-[r2:T1]->(b)
    val selections =
      Selections.from(In(Variable("a")(pos), Property(Variable("r")(pos), PropertyKeyName("foo")(pos))(pos))(pos))
    val qg = QueryGraph(
      patternRelationships =
        Set(PatternRelationship(
          "r",
          ("a", "b"),
          SemanticDirection.OUTGOING,
          Seq(RelTypeName("T1")(pos)),
          SimplePatternLength
        )),
      selections = selections
    )
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        CreatePattern(
          Nil,
          List(
            CreateRelationship(
              "r2",
              "a",
              RelTypeName("T1")(pos),
              "b",
              SemanticDirection.OUTGOING,
              Some(
                MapExpression(Seq(
                  (PropertyKeyName("foo")(pos), SignedDecimalIntegerLiteral("42")(pos))
                ))(pos)
              )
            )
          )
        )
      )
    )

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("overlap when reading, deleting and merging") {
    // MATCH (a:L1:L2) DELETE a CREATE (b:L3)
    val selections =
      Selections.from(HasLabels(Variable("a")(pos), Seq(LabelName("L1")(pos), LabelName("L2")(pos)))(pos))
    val qg = QueryGraph(patternNodes = Set("a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        DeleteExpression(Variable("a")(pos), forced = false),
        MergeNodePattern(
          CreateNode("b", Set(LabelName("L3")(pos), LabelName("L3")(pos)), None),
          QueryGraph.empty,
          Seq.empty,
          Seq.empty
        )
      )
    )

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe ListSet(EagernessReason.ReadDeleteConflict("a"))
  }

  test("overlap when reading and deleting with collections") {
    // ... WITH collect(a) as col DELETE col[0]
    val qg = QueryGraph(argumentIds = Set("col"))
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        DeleteExpression(Variable("col")(pos), forced = false)
      )
    )

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe ListSet(EagernessReason.ReadDeleteConflict("col"))
  }

  test("overlap when reading and merging on the same label and property") {
    // MATCH (a:Label {prop: 42}) MERGE (b:Label {prop: 123})
    val selections = Selections.from(Seq(
      hasLabels("a", "Label"),
      in(prop("a", "prop"), listOfInt(42))
    ))
    val qg = QueryGraph(patternNodes = Set("a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        MergeNodePattern(
          CreateNode("b", Set(labelName("Label")), Some(mapOfInt("prop" -> 123))),
          QueryGraph.empty,
          Seq.empty,
          Seq.empty
        )
      )
    )

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("overlap when reading and merging on the same property, no label on MATCH") {
    // MATCH (a {prop: 42}) MERGE (b:Label {prop: 123})
    val selections = Selections.from(Seq(
      in(prop("a", "prop"), listOfInt(42))
    ))
    val qg = QueryGraph(patternNodes = Set("a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        MergeNodePattern(
          CreateNode("b", Set(labelName("Label")), Some(mapOfInt("prop" -> 123))),
          QueryGraph.empty,
          Seq.empty,
          Seq.empty
        )
      )
    )

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe ListSet(EagernessReason.Unknown)
  }

  test("no overlap when reading and merging on the same property but different labels") {
    // MATCH (a:Label {prop: 42}) MERGE (b:OtherLabel {prop: 123})
    val selections = Selections.from(Seq(
      hasLabels("a", "Label"),
      in(prop("a", "prop"), listOfInt(42))
    ))
    val qg = QueryGraph(patternNodes = Set("a"), selections = selections)
    val ug = QueryGraph(mutatingPatterns =
      IndexedSeq(
        MergeNodePattern(
          CreateNode("b", Set(labelName("OtherLabel")), Some(mapOfInt("prop" -> 123))),
          QueryGraph.empty,
          Seq.empty,
          Seq.empty
        )
      )
    )

    ug.overlaps(qgWithNoStableIdentifierAndOnlyLeaves(qg)) shouldBe empty
  }

  test("allQueryGraphs should include IRExpressions recursively") {
    val innerQg1 = QueryGraph(patternNodes = Set("a"))
    val innerQg2 = QueryGraph(patternNodes = Set("b"))
    val qg = QueryGraph(
      selections = Selections.from(Seq(
        ExistsIRExpression(PlannerQuery(RegularSinglePlannerQuery(innerQg1)), "")(pos),
        ListIRExpression(PlannerQuery(RegularSinglePlannerQuery(innerQg2)), "", "", "")(pos)
      ))
    )

    qg.allQGsWithLeafInfo.map(_.queryGraph) should (contain(innerQg1) and contain(innerQg2))
  }

  test("allQueryGraphs in horizon should include IRExpressions recursively") {
    val innerQg1 = QueryGraph(patternNodes = Set("a"))
    val innerQg2 = QueryGraph(patternNodes = Set("b"))
    val horizon = RegularQueryProjection(
      Map(
        "a" -> ExistsIRExpression(PlannerQuery(RegularSinglePlannerQuery(innerQg1)), "")(pos),
        "b" -> ListIRExpression(PlannerQuery(RegularSinglePlannerQuery(innerQg2)), "", "", "")(pos)
      )
    )

    horizon.allQueryGraphs.map(_.queryGraph) should (contain(innerQg1) and contain(innerQg2))
  }

  private def createNode(name: String, labels: String*) =
    CreatePattern(List(CreateNode(name, labels.map(l => LabelName(l)(pos)).toSet, None)), Nil)

  private def createRelationship(name: String, start: String, relType: String, end: String) =
    CreatePattern(
      Nil,
      List(CreateRelationship(name, start, RelTypeName(relType)(pos), end, SemanticDirection.OUTGOING, None))
    )
}
