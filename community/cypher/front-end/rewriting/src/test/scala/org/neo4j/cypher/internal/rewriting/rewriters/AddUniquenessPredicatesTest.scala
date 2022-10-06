/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.expressions.LabelExpression
import org.neo4j.cypher.internal.expressions.LabelExpression.ColonConjunction
import org.neo4j.cypher.internal.expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.expressions.LabelExpression.Conjunctions
import org.neo4j.cypher.internal.expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.expressions.LabelExpression.Negation
import org.neo4j.cypher.internal.expressions.LabelExpression.Wildcard
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.rewriting.RewriteTest
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates.evaluate
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates.getRelTypesToConsider
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalactic.anyvals.PosZInt
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.annotation.tailrec

class AddUniquenessPredicatesTest extends CypherFunSuite with RewriteTest with AstConstructionTestSupport {

  private def disjoint(lhs: String, rhs: String): String =
    s"NONE(`  UNNAMED0` IN $lhs WHERE `  UNNAMED0` IN $rhs)"

  private def unique(rhs: String, unnamedOffset: Int = 0): String =
    s"ALL(`  UNNAMED$unnamedOffset` IN $rhs WHERE SINGLE(`  UNNAMED${unnamedOffset + 1}` IN $rhs WHERE `  UNNAMED$unnamedOffset` = `  UNNAMED${unnamedOffset + 1}`))"

  test("does not introduce predicate not needed") {
    assertIsNotRewritten("RETURN 42")
    assertIsNotRewritten("MATCH (n) RETURN n")
    assertIsNotRewritten("MATCH (n)-[r1]->(m) RETURN n")
    assertIsNotRewritten("MATCH (n)-[r1]->(m) MATCH (m)-[r2]->(x) RETURN x")
  }

  test("uniqueness check is done for one variable length relationship") {
    assertRewrite(
      "MATCH (b)-[r*0..1]->(c) RETURN *",
      s"MATCH (b)-[r*0..1]->(c) WHERE ${unique("r")} RETURN *"
    )
  }

  test("uniqueness check is done between relationships of simple and variable pattern lengths") {
    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2*0..1]->(c) RETURN *",
      s"MATCH (a)-[r1]->(b)-[r2*0..1]->(c) WHERE NOT r1 IN r2 AND ${unique("r2")} RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1*0..1]->(b)-[r2]->(c) RETURN *",
      s"MATCH (a)-[r1*0..1]->(b)-[r2]->(c) WHERE NOT r2 IN r1 AND ${unique("r1")} RETURN *"
    )
  }

  test("no uniqueness check between relationships of simple and variable pattern lengths of different type") {
    assertRewrite(
      "MATCH (a)-[r1:R1]->(b)-[r2:R2*0..1]->(c) RETURN *",
      s"MATCH (a)-[r1:R1]->(b)-[r2:R2*0..1]->(c) WHERE ${unique("r2")} RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1:R1*0..1]->(b)-[r2:R2]->(c) RETURN *",
      s"MATCH (a)-[r1:R1*0..1]->(b)-[r2:R2]->(c) WHERE ${unique("r1")} RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1:R1]->(b)-[r2:R2*0..1]->(c)-[r3:R1|R2*0..1]->(d) RETURN *",
      s"""MATCH (a)-[r1:R1]->(b)-[r2:R2*0..1]->(c)-[r3:R1|R2*0..1]->(d) 
         |WHERE ${disjoint("r3", "r2")} AND NOT r1 IN r3 AND ${unique("r3", 1)} AND ${unique("r2", 3)} 
         |RETURN *""".stripMargin
    )
  }

  test("uniqueness check is done between relationships of variable and variable pattern lengths") {
    assertRewrite(
      "MATCH (a)-[r1*0..1]->(b)-[r2*0..1]->(c) RETURN *",
      s"""MATCH (a)-[r1*0..1]->(b)-[r2*0..1]->(c)
         |WHERE ${disjoint("r2", "r1")} AND ${unique("r2", 1)} AND ${unique("r1", 3)}
         |RETURN *""".stripMargin
    )
  }

  test("no uniqueness check between relationships of variable and variable pattern lengths of different type") {
    assertRewrite(
      "MATCH (a)-[r1:R1*0..1]->(b)-[r2:R2*0..1]->(c) RETURN *",
      s"""MATCH (a)-[r1:R1*0..1]->(b)-[r2:R2*0..1]->(c) 
         |WHERE ${unique("r2")} AND ${unique("r1", 2)} 
         |RETURN *""".stripMargin
    )
  }

  test("uniqueness check is done between relationships") {
    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c)-[r3]->(d) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c)-[r3]->(d) WHERE NOT(r3 = r2) AND NOT(r3 = r1) AND NOT(r2 = r1) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b), (b)-[r2]->(c), (c)-[r3]->(d) RETURN *",
      "MATCH (a)-[r1]->(b), (b)-[r2]->(c), (c)-[r3]->(d) WHERE NOT(r1 = r2) AND NOT(r1 = r3) AND NOT(r2 = r3) RETURN *"
    )
  }

  test("no uniqueness check between relationships of different type") {
    assertIsNotRewritten("MATCH (a)-[r1:X]->(b)-[r2:Y]->(c) RETURN *")

    assertRewrite(
      "MATCH (a)-[r1:X]->(b)-[r2:X|Y]->(c) RETURN *",
      "MATCH (a)-[r1:X]->(b)-[r2:X|Y]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2:X]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2:X]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2:%]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2:%]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1:%]->(b)-[r2:%]->(c) RETURN *",
      "MATCH (a)-[r1:%]->(b)-[r2:%]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertIsNotRewritten("MATCH (a)-[r1]->(b)-[r2:!%]->(c) RETURN *")

    assertIsNotRewritten("MATCH (a)-[r1:X]->(b)-[r2:!X]->(c) RETURN *")

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2:!X]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2:!X]->(c) WHERE NOT(r2 = r1) RETURN *"
    )

    assertIsNotRewritten("MATCH (a)-[r1:A&B]->(b)-[r2:B&C]->(c) RETURN *")
  }

  test("uniqueness check is done for a single length-one QPP") {
    assertRewrite(
      "MATCH (b) (()-[r]->()){0,1} (c) RETURN *",
      s"MATCH (b) (()-[r]->()){0,1} (c) WHERE ${unique("r")} RETURN *"
    )
  }

  test(
    "uniqueness check for a single length-one QPP with unnamed relationship variable promotes unnamed variable to grouping"
  ) {
    // GIVEN
    val query = "MATCH (b) (()-->(m)){0,1} (c) RETURN *"
    val parsed = parseForRewriting(query)
    SemanticChecker.check(parsed)

    // WHEN
    val result = parsed.endoRewrite(inSequence(
      nameAllPatternElements(new AnonymousVariableNameGenerator),
      AddUniquenessPredicates
    ))

    // THEN
    val qpp = result.folder.treeFindByClass[QuantifiedPath].get
    // all three elements of the QPP should be included in the grouping
    qpp.variableGroupings should have size 2 // the relationship and m, but not the anonymous node
    // ... and have the right position (that of the qpp)
    qpp.variableGroupings.map(_.group.position).foreach { pos =>
      pos should equal(qpp.position)
    }

    // As the unique(...) predicate references a relationship group variable from the qpp, we need to make sure that this has the right position
    // so that the semantic table can correctly infer this to be a list of relationships and does not confuse it with the variable inside the qpp
    // which is of type relationship.
    val unique = result.folder.treeFindByClass[Unique].get
    unique.rhs.position should be(qpp.position)
  }

  test("uniqueness check is done for a single length-two QPP") {
    assertRewrite(
      "MATCH (b) (()-[r1]->()-[r2]->()){0,1} (c) RETURN *",
      s"MATCH (b) (()-[r1]->()-[r2]->() WHERE NOT (r2 = r1)){0,1} (c) WHERE ${unique("r1 + r2")} RETURN *"
    )
  }

  test("uniqueness check is done for a single length-three QPP") {
    assertRewrite(
      "MATCH (b) (()-[r1]->()-[r2]->()-[r3]->()){0,1} (c) RETURN *",
      s"""MATCH (b) (()-[r1]->()-[r2]->()-[r3]->() WHERE NOT(r3 = r2) AND NOT(r3 = r1) AND NOT(r2 = r1)){0,1} (c)
         |WHERE ${unique("r1 + r2 + r3")}
         |RETURN *""".stripMargin
    )
  }

  test("uniqueness check is done between relationships of simple lengths and QPPs") {
    assertRewrite(
      "MATCH (a)-[r1]->(b) (()-[r2]->())* RETURN *",
      s"MATCH (a)-[r1]->(b) (()-[r2]->())* WHERE NOT r1 IN r2 AND ${unique("r2")} RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b) (()-[r2]->()-[r3]->())* RETURN *",
      s"""MATCH (a)-[r1]->(b) (()-[r2]->()-[r3]->() WHERE NOT (r3 = r2))* 
         |WHERE NOT r1 IN (r2 + r3) AND ${unique("r2 + r3")}
         |RETURN *""".stripMargin
    )

    assertRewrite(
      "MATCH ((a)-[r1]->())* (b)-[r2]->(c) RETURN *",
      s"MATCH ((a)-[r1]->())* (b)-[r2]->(c) WHERE NOT r2 IN r1 AND ${unique("r1")} RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b) (()-[r2]->()-[r3]->())* (c)-[r4]->(d) RETURN *",
      s"""MATCH (a)-[r1]->(b) (()-[r2]->()-[r3]->() WHERE NOT (r3 = r2))* (c)-[r4]->(d)
         |WHERE NOT r1 IN (r2 + r3) AND NOT r1 = r4 AND NOT r4 IN (r2 + r3) AND ${unique("r2 + r3")}
         |RETURN *""".stripMargin
    )
  }

  test("no uniqueness check between relationships of simple lengths and QPPs of different type") {
    assertRewrite(
      "MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->())* RETURN *",
      s"MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->())* WHERE ${unique("r2")} RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->()-[r3:R3]->())* RETURN *",
      s"""MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->()-[r3:R3]->())* 
         |WHERE ${unique("r2 + r3")}
         |RETURN *""".stripMargin
    )

    assertRewrite(
      "MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->()-[r3:R1]->())* RETURN *",
      s"""MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->()-[r3:R1]->())* 
         |WHERE NOT r1 IN r3 AND ${unique("r2 + r3")}
         |RETURN *""".stripMargin
    )

    assertRewrite(
      "MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->())* (()-[r3:R1]->())* RETURN *",
      s"""MATCH (a)-[r1:R1]->(b) (()-[r2:R2]->())* (()-[r3:R1]->())*
         |WHERE NOT r1 IN r3 AND ${unique("r2")} AND ${unique("r3", 2)}
         |RETURN *""".stripMargin
    )
  }

  test("uniqueness check is done between QPPs and QPPs") {
    assertRewrite(
      "MATCH (()-[r1]->())+ (()-[r2]->())+ RETURN *",
      s"""MATCH (()-[r1]->())+ (()-[r2]->())+ 
         |WHERE ${disjoint("r1", "r2")} AND ${unique("r1", 1)} AND ${unique("r2", 3)}
         |RETURN *""".stripMargin
    )

    assertRewrite(
      "MATCH (()-[r1]->()-[r2]->())+ (()-[r3]->()-[r4]->())+ RETURN *",
      s"""MATCH (()-[r1]->()-[r2]->() WHERE NOT (r2 = r1))+ (()-[r3]->()-[r4]->() WHERE NOT (r4 = r3))+ 
         |WHERE ${disjoint("r1 + r2", "r3 + r4")} AND ${unique("r1 + r2", 1)} AND ${unique("r3 + r4", 3)}
         |RETURN *""".stripMargin
    )
  }

  test("no uniqueness check between QPPs and QPPs of different type") {
    assertRewrite(
      "MATCH (()-[r1:R1]->())+ (()-[r2:R2]->())+ RETURN *",
      s"""MATCH (()-[r1:R1]->())+ (()-[r2:R2]->())+ 
         |WHERE ${unique("r1")} AND ${unique("r2", 2)}
         |RETURN *""".stripMargin
    )

    // Here there is no overlap between the first and the second QPP, so no need for a disjoint.
    assertRewrite(
      "MATCH (()-[r1:R1]->()-[r2:R2]->())+ (()-[r3:R3]->()-[r4:R4]->())+ RETURN *",
      s"""MATCH (()-[r1:R1]->()-[r2:R2]->())+ (()-[r3:R3]->()-[r4:R4]->())+ 
         |WHERE ${unique("r1 + r2")} AND ${unique("r3 + r4", 2)}
         |RETURN *""".stripMargin
    )

    // Here relationships overlap pairwise.
    // But since the trail operator puts everything into one big set anyway, we put all relationships in disjoint.
    assertRewrite(
      "MATCH (()-[r1:R1]->()-[r2:R2]->())+ (()-[r3:R1]->()-[r4:R2]->())+ RETURN *",
      s"""MATCH (()-[r1:R1]->()-[r2:R2]->())+ (()-[r3:R1]->()-[r4:R2]->())+ 
         |WHERE ${disjoint("r1 + r2", "r3 + r4")} AND ${unique("r1 + r2", 1)} AND ${unique("r3 + r4", 3)}
         |RETURN *""".stripMargin
    )

    // Here some relationships overlap.
    assertRewrite(
      "MATCH (()-[r1:R1]->()-[r2:R2]->())+ (()-[r3:R1]->()-[r4:R4]->())+ RETURN *",
      s"""MATCH (()-[r1:R1]->()-[r2:R2]->())+ (()-[r3:R1]->()-[r4:R4]->())+ 
         |WHERE ${disjoint("r1", "r3")} AND ${unique("r1 + r2", 1)} AND ${unique("r3 + r4", 3)}
         |RETURN *""".stripMargin
    )
  }

  test("getRelTypesToConsider should return all relevant relationship types") {
    getRelTypesToConsider(None) shouldEqual Seq(relTypeName(""))

    getRelTypesToConsider(Some(labelRelTypeLeaf("A"))) should contain theSameElementsAs Seq(
      relTypeName(""),
      relTypeName("A")
    )

    getRelTypesToConsider(Some(
      labelConjunction(
        labelRelTypeLeaf("A"),
        labelDisjunction(labelNegation(labelRelTypeLeaf("B")), labelRelTypeLeaf("C"))
      )
    )) should contain theSameElementsAs Seq(relTypeName(""), relTypeName("A"), relTypeName("B"), relTypeName("C"))
  }

  test("overlaps") {
    evaluate(labelRelTypeLeaf("A"), relTypeName("A")).result shouldBe true

    evaluate(labelDisjunction(labelRelTypeLeaf("A"), labelRelTypeLeaf("B")), relTypeName("B")).result shouldBe true
    evaluate(labelConjunction(labelRelTypeLeaf("A"), labelRelTypeLeaf("B")), relTypeName("B")).result shouldBe false
    evaluate(labelConjunction(labelWildcard(), labelRelTypeLeaf("B")), relTypeName("B")).result shouldBe true
    evaluate(
      labelConjunction(labelNegation(labelRelTypeLeaf("A")), labelRelTypeLeaf("B")),
      relTypeName("B")
    ).result shouldBe true
  }

  test("ignores shortestPath relationships for uniqueness") {
    assertRewrite(
      "MATCH (a)-[r1]->(b), shortestPath((a)-[r*]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b), shortestPath((a)-[r*]->(b)) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c), shortestPath((a)-[r*]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c), shortestPath((a)-[r*]->(b)) WHERE not(r2 = r1) RETURN *"
    )
  }

  test("ignores allShortestPaths relationships for uniqueness") {
    assertRewrite(
      "MATCH (a)-[r1]->(b), allShortestPaths((a)-[r*]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b), allShortestPaths((a)-[r*]->(b)) RETURN *"
    )

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c), allShortestPaths((a)-[r*]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c), allShortestPaths((a)-[r*]->(b)) WHERE not(r2 = r1) RETURN *"
    )
  }

  def rewriterUnderTest: Rewriter = inSequence(
    AddUniquenessPredicates,
    UniquenessRewriter(new AnonymousVariableNameGenerator)
  )
}

class AddUniquenessPredicatesPropertyTest extends CypherFunSuite with ScalaCheckPropertyChecks
    with RelationshipTypeExpressionGenerators {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(
      minSuccessful = 100,
      minSize = PosZInt(0),
      sizeRange = PosZInt(20)
    )

  test("overlaps is commutative") {
    forAll { (expression1: RelationshipTypeExpression, expression2: RelationshipTypeExpression) =>
      expression1.overlaps(expression2) shouldEqual expression2.overlaps(expression1)
    }
  }

  test("never overlap with nothing") {
    forAll { (expression: RelationshipTypeExpression) =>
      expression.overlaps(!wildcard) shouldBe false
    }
  }

  test("overlaps boolean logic") {
    forAll {
      (
        expression1: RelationshipTypeExpression,
        expression2: RelationshipTypeExpression,
        expression3: RelationshipTypeExpression
      ) =>
        val doesOverlap = expression1.overlaps(expression2)
        if (doesOverlap)
          withClue("expression1.overlaps(expression2) ==> expression1.overlaps(expression2.or(expression3))") {
            expression1.overlaps(expression2.or(expression3)) shouldBe true
          }
        else
          withClue("!expression1.overlaps(expression2) ==> !expression1.overlaps(expression2.and(expression3))") {
            expression1.overlaps(expression2.and(expression3)) shouldBe false
          }
    }
  }

  test("overlaps is stack-safe") {
    @tailrec
    def buildExpression(i: Int, expression: RelationshipTypeExpression): RelationshipTypeExpression =
      if (i <= 0) expression else buildExpression(i - 1, !expression)

    buildExpression(10_000, wildcard).overlaps(wildcard) shouldBe true
  }
}

trait RelationshipTypeExpressionGenerators {

  /**
   * Finite (small) set of names used to build arbitrary relationship type expressions.
   * It's all Greek to me.
   * Keeping it small and hard-coded ensures that the expressions will contain overlaps
   */
  val names = Set("ALPHA", "BETA", "GAMMA", "DELTA", "EPSILON")

  val position: InputPosition = InputPosition.NONE

  /**
   * Wrapper type around a [[LabelExpression]] that can be found in a relationship pattern
   * @param value Underlying [[LabelExpression]] that doesn't contain any [[Label]] or [[LabelOrRelType]]
   */
  case class RelationshipTypeExpression(value: LabelExpression) {

    def overlaps(other: RelationshipTypeExpression): Boolean = {
      val allTypes = AddUniquenessPredicates.getRelTypesToConsider(Some(value)).concat(
        AddUniquenessPredicates.getRelTypesToConsider(Some(other.value))
      )
      (AddUniquenessPredicates.overlaps(allTypes, Some(value)) intersect AddUniquenessPredicates.overlaps(
        allTypes,
        Some(other.value)
      )).nonEmpty
    }

    def unary_! : RelationshipTypeExpression =
      RelationshipTypeExpression(Negation(value)(position))

    def and(other: RelationshipTypeExpression): RelationshipTypeExpression =
      RelationshipTypeExpression(Conjunctions(Seq(value, other.value))(position))

    def or(other: RelationshipTypeExpression): RelationshipTypeExpression =
      RelationshipTypeExpression(Disjunctions(Seq(value, other.value))(position))
  }

  val wildcard: RelationshipTypeExpression = RelationshipTypeExpression(Wildcard()(position))

  val genWildCard: Gen[Wildcard] = Gen.const(Wildcard()(position))

  val genRelType: Gen[Leaf] =
    Gen.oneOf(names.toSeq).map(name => Leaf(RelTypeName(name)(position)))

  def genBinary[A](f: (LabelExpression, LabelExpression) => A): Gen[A] =
    Gen.sized(size =>
      for {
        lhs <- Gen.resize(size / 2, genLabelExpression)
        rhs <- Gen.resize(size / 2, genLabelExpression)
      } yield f(lhs, rhs)
    )

  val genConjunction: Gen[Conjunctions] =
    genBinary((lhs, rhs) => Conjunctions(Seq(lhs, rhs))(position))

  val genColonConjunction: Gen[ColonConjunction] =
    genBinary((lhs, rhs) => ColonConjunction(lhs, rhs)(position))

  val genDisjunction: Gen[Disjunctions] =
    genBinary((lhs, rhs) => Disjunctions(Seq(lhs, rhs))(position))

  val genColonDisjunction: Gen[ColonDisjunction] =
    genBinary((lhs, rhs) => ColonDisjunction(lhs, rhs)(position))

  val genNegation: Gen[Negation] =
    Gen.sized(size => Gen.resize(size - 1, genLabelExpression)).map(Negation(_)(position))

  val genLabelExpression: Gen[LabelExpression] =
    Gen.sized(size =>
      if (size <= 0)
        Gen.oneOf(
          genWildCard,
          genRelType
        )
      else
        Gen.oneOf(
          genConjunction,
          genColonConjunction,
          genDisjunction,
          genColonDisjunction,
          genNegation,
          genWildCard,
          genRelType
        )
    )

  implicit val arbitraryRelationshipTypeExpression: Arbitrary[RelationshipTypeExpression] =
    Arbitrary(genLabelExpression.map(RelationshipTypeExpression))
}
