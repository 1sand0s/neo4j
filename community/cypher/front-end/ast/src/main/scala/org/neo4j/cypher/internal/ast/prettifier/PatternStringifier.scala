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
package org.neo4j.cypher.internal.ast.prettifier

import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FixedQuantifier
import org.neo4j.cypher.internal.expressions.IntervalQuantifier
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.ShortestPaths
import org.neo4j.cypher.internal.expressions.StarQuantifier

trait PatternStringifier {
  def apply(p: Pattern): String
  def apply(p: PatternPart): String
  def apply(element: PatternElement): String
  def apply(nodePattern: NodePattern): String
  def apply(relationshipChain: RelationshipChain): String
  def apply(relationship: RelationshipPattern): String
  def apply(concatenation: PathConcatenation): String
  def apply(quantified: QuantifiedPath): String
  def apply(path: ParenthesizedPath): String
}

object PatternStringifier {
  def apply(expr: ExpressionStringifier): PatternStringifier = new DefaultPatternStringifier(expr)
}

private class DefaultPatternStringifier(expr: ExpressionStringifier) extends PatternStringifier {

  override def apply(p: Pattern): String =
    p.patternParts.map(apply).mkString(", ")

  override def apply(p: PatternPart): String = p match {
    case e: EveryPath        => apply(e.element)
    case s: ShortestPaths    => s"${s.name}(${apply(s.element)})"
    case n: NamedPatternPart => s"${expr(n.variable)} = ${apply(n.patternPart)}"
  }

  override def apply(element: PatternElement): String = element match {
    case r: RelationshipChain => apply(r)
    case n: NodePattern       => apply(n)
    case c: PathConcatenation => apply(c)
    case q: QuantifiedPath    => apply(q)
    case p: ParenthesizedPath => apply(p)
  }

  override def apply(nodePattern: NodePattern): String = {
    val variable = nodePattern.variable.map(expr(_))

    val labelExpression =
      nodePattern.labelExpression
        .map(le => {
          val isOrColon = if (le.containsIs) " IS " else ":"
          s"$isOrColon${expr.stringifyLabelExpression(le)}"
        })

    val body =
      concatenate(
        " ",
        Seq(
          concatenate("", Seq(variable, labelExpression)),
          nodePattern.properties.map(expr(_)),
          nodePattern.predicate.map(stringifyPredicate)
        )
      ).getOrElse("")

    s"($body)"
  }

  override def apply(relationshipChain: RelationshipChain): String = {
    val r = apply(relationshipChain.rightNode)
    val middle = apply(relationshipChain.relationship)
    val l = apply(relationshipChain.element)

    s"$l$middle$r"
  }

  override def apply(relationship: RelationshipPattern): String = {
    val variable = relationship.variable.map(expr(_))

    val labelExpression =
      relationship.labelExpression
        .map(le => {
          val isOrColon = if (le.containsIs) " IS " else ":"
          s"$isOrColon${expr.stringifyLabelExpression(le)}"
        })

    val length = relationship.length match {
      case None              => None
      case Some(None)        => Some("*")
      case Some(Some(range)) => Some(stringifyRange(range))
    }

    val body = concatenate(
      " ",
      Seq(
        concatenate("", Seq(variable, labelExpression, length)),
        relationship.properties.map(expr(_)),
        relationship.predicate.map(stringifyPredicate)
      )
    ).fold("")(inner => s"[$inner]")

    relationship.direction match {
      case SemanticDirection.OUTGOING => s"-$body->"
      case SemanticDirection.INCOMING => s"<-$body-"
      case SemanticDirection.BOTH     => s"-$body-"
    }
  }

  override def apply(concatenation: PathConcatenation): String =
    concatenation.factors.map(apply).mkString(" ")

  override def apply(quantified: QuantifiedPath): String = {
    val pattern = apply(quantified.part)
    val where = quantified.optionalWhereExpression
      .map(stringifyPredicate)
      .map(w => s" $w")
      .getOrElse("")
    val quantifier = quantified.quantifier match {
      case StarQuantifier() => "*"
      case PlusQuantifier() => "+"
      case IntervalQuantifier(lower, upper) =>
        s"{${lower.map(_.stringVal).getOrElse("")}, ${upper.map(_.stringVal).getOrElse("")}}"
      case FixedQuantifier(value) => s"{${value.stringVal}}"
    }
    s"($pattern$where)$quantifier"
  }

  override def apply(path: ParenthesizedPath): String = {
    val inner = apply(path.part)
    s"($inner)"
  }

  private def concatenate(separator: String, fragments: Seq[Option[String]]): Option[String] =
    Some(fragments.flatten)
      .filter(_.nonEmpty) // ensures that there is at least one fragment
      .map(_.mkString(separator))

  private def stringifyRange(range: Range): String = {
    val lower = range.lower.fold("")(_.stringVal)
    val upper = range.upper.fold("")(_.stringVal)
    s"*$lower..$upper"
  }

  private def stringifyPredicate(predicate: Expression): String =
    s"WHERE ${expr(predicate)}"

}
