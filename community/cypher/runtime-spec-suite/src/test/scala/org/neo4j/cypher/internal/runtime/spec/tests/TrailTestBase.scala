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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType

import java.util.Collections.emptyList

abstract class TrailTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  private val `(me:START) [(a)-[r]->(b)]{0,2} (you)` : TrailParameters = TrailParameters(
    min = 0,
    max = Limited(2),
    start = "me",
    end = "you",
    innerStart = "a_inner",
    innerEnd = "b_inner",
    groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
    groupRelationships = Set(("r_inner", "r")),
    innerRelationships = Set("r_inner"),
    previouslyBoundRelationships = Set.empty,
    previouslyBoundRelationshipGroups = Set.empty
  )

  test("should respect upper limit") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me:START) [(a)-[r]->(b)]{0,2} (you)`)
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23))
      )
    )
  }

  test("should handle unused anonymous end-node") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val `(me:START) [(a)-[r]->(b)]{0,2}` = TrailParameters(
      min = 0,
      max = Limited(2),
      start = "me",
      end = "anon",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "a", "b", "r")
      .trail(`(me:START) [(a)-[r]->(b)]{0,2}`)
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "a", "b", "r").withRows(
      Seq(
        Array(n1, emptyList(), emptyList(), emptyList()),
        Array(n1, listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23))
      )
    )
  }

  test("should respect lower limit") {
    // (n1:START) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph
    val `(me:START) [(a)-[r]->(b)]{2,2} (you)` = TrailParameters(
      min = 2,
      max = Limited(2),
      start = "me",
      end = "you",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me:START) [(a)-[r]->(b)]{2,2} (you)`)
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23))
      )
    )
  }

  test("should respect relationship uniqueness") {
    // given
    //          (n1)
    //        ↗     ↘
    //     (n4)     (n2)
    //        ↖     ↙
    //          (n3)
    val (n1, n2, n3, n4, r12, r23, r34, r41) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r41 = n4.createRelationshipTo(n1, RelationshipType.withName("R"))
      (n1, n2, n3, n4, r12, r23, r34, r41)
    }
    val `(me:START) [(a)-[r]->(b)]{0, *} (you)` = TrailParameters(
      min = 0,
      max = Unlimited,
      start = "me",
      end = "you",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me:START) [(a)-[r]->(b)]{0, *} (you)`)
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
        Array(n1, n4, listOf(n1, n2, n3), listOf(n2, n3, n4), listOf(r12, r23, r34)),
        Array(n1, n1, listOf(n1, n2, n3, n4), listOf(n2, n3, n4, n1), listOf(r12, r23, r34, r41))
      )
    )
  }

  test("should respect relationship uniqueness of several relationship variables") {
    // given
    //          (n1)
    //        ↗     ↘
    //      (n5)
    //        |
    //     (n4)     (n2)
    //        ↖     ↙
    //          (n3)
    val (n1, n2, n3, n4, n5, r12, r23, r34, r45, r51) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val n5 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r45 = n4.createRelationshipTo(n5, RelationshipType.withName("R"))
      val r51 = n5.createRelationshipTo(n1, RelationshipType.withName("R"))
      (n1, n2, n3, n4, n5, r12, r23, r34, r45, r51)
    }
    val `(me:START) [(a)-[r]->()-[]->(b)]{0, *} (you)` = TrailParameters(
      min = 0,
      max = Unlimited,
      start = "me",
      end = "you",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner", "ranon"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me:START) [(a)-[r]->()-[]->(b)]{0, *} (you)`)
      .|.expandAll("(secret)-[ranon]->(b_inner)")
      .|.expandAll("(a_inner)-[r_inner]->(secret)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n3, listOf(n1), listOf(n3), listOf(r12)),
        Array(n1, n5, listOf(n1, n3), listOf(n3, n5), listOf(r12, r34))
      )
    )
  }

  test("should handle branched graph") {
    //      (n2) → (n4)
    //     ↗
    // (n1)
    //     ↘
    //      (n3) → (n5)
    val (n1, n2, n3, n4, n5, r12, r13, r24, r35) = given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val n4 = tx.createNode()
      val n5 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r13 = n1.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r24 = n2.createRelationshipTo(n4, RelationshipType.withName("R"))
      val r35 = n3.createRelationshipTo(n5, RelationshipType.withName("R"))
      (n1, n2, n3, n4, n5, r12, r13, r24, r35)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me:START) [(a)-[r]->(b)]{0,2} (you)`)
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()), // 0
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)), // 1
        Array(n1, n3, listOf(n1), listOf(n3), listOf(r13)), // 1
        Array(n1, n4, listOf(n1, n2), listOf(n2, n4), listOf(r12, r24)), // 2
        Array(n1, n5, listOf(n1, n3), listOf(n3, n5), listOf(r13, r35)) // 2
      )
    )
  }

  test("should work for the zero length case") {
    // (n1) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val `(me:START) [(a)-[r]->(b)]{0,0} (you)` = TrailParameters(
      min = 0,
      max = Limited(0),
      start = "me",
      end = "you",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me:START) [(a)-[r]->(b)]{0,0} (you)`)
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList())
      )
    )
  }

  test("should be able to reference LHS from RHS") {
    // (n1) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = given {
      val n1 = tx.createNode()
      n1.setProperty("prop", 1)
      val n2 = tx.createNode()
      n2.setProperty("prop", 1)
      val n3 = tx.createNode()
      n3.setProperty("prop", 42)
      val n4 = tx.createNode()
      n4.setProperty("prop", 42)
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r34 = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
      (n1, n2, n3, n4, r12, r23, r34)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r")
      .trail(`(me:START) [(a)-[r]->(b)]{0,2} (you)`)
      .|.filter("b_inner.prop = me.prop")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .allNodeScan("me")
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12)),
        Array(n2, n2, emptyList(), emptyList(), emptyList()),
        Array(n3, n3, emptyList(), emptyList(), emptyList()),
        Array(n3, n4, listOf(n3), listOf(n4), listOf(r34)),
        Array(n4, n4, emptyList(), emptyList(), emptyList())
      )
    )
  }

  test("should work when columns are introduced on top of trail") {
    // (n1) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "r2")
      .projection("r AS r2")
      .trail(`(me:START) [(a)-[r]->(b)]{0,2} (you)`)
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "r2").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList()),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), listOf(r12)),
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(r12, r23))
      )
    )
  }

  test("should work when concatenated") {
    // (n1) → (n2) → (n3) → (n4)
    val (n1, n2, n3, n4, r12, r23, r34) = smallChainGraph

    // given: (me:START) [(a)-[r]->(b)]{0,1} [(c)-[rr]->(d)]{0,1} (you)
    // which becomes: (me:START) [(a)-[r]->(b)]{0,1} (anon) [(c)-[rr]->(d)]{0,1} (you)

    val `(anon) [(c)-[rr]->(d)]{0,1} (you)` = TrailParameters(
      min = 0,
      max = Limited(1),
      start = "anon",
      end = "you",
      innerStart = "c_inner",
      innerEnd = "d_inner",
      groupNodes = Set(("c_inner", "c"), ("d_inner", "d")),
      groupRelationships = Set(("rr_inner", "rr")),
      innerRelationships = Set("rr_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set("r")
    )
    val `(me:START) [(a)-[r]->(b)]{0,1} (anon)` = TrailParameters(
      min = 0,
      max = Limited(1),
      start = "me",
      end = "anon",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "c", "d", "rr")
      .trail(`(anon) [(c)-[rr]->(d)]{0,1} (you)`)
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("me", "anon", "c_inner")
      .trail(`(me:START) [(a)-[r]->(b)]{0,1} (anon)`)
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // 0: (n1)
    // 1: (n1) → (n2)

    // 0: (n1)
    // 0: (n1) → (n2)
    // 1: (n1) → (n2)
    // 1: (n1) → (n2) → (n3)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "c", "d", "rr").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
        Array(n1, n2, emptyList(), emptyList(), emptyList(), listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), emptyList(), emptyList(), emptyList()),
        Array(n1, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23))
      )
    )
  }

  test("should respect relationship uniqueness when concatenated") {
    // given
    //          (n1)
    //        ↗     ↘
    //      (n3) <- (n2)
    val (n1, n2, n3, r12, r23, r31) = smallCircularGraph

    // given: (me:START) [(a)-[r]->(b)]{0,2} [(c)-[rr]->(d)]{0,2} (you)
    // which becomes: (me:START) [(a)-[r]->(b)]{0,2} (anon) [(c)-[rr]->(d)]{0,2} (you)

    val `(anon) [(c)-[rr]->(d)]{0,2} (you)` = TrailParameters(
      min = 0,
      max = Limited(2),
      start = "anon",
      end = "you",
      innerStart = "c_inner",
      innerEnd = "d_inner",
      groupNodes = Set(("c_inner", "c"), ("d_inner", "d")),
      groupRelationships = Set(("rr_inner", "rr")),
      innerRelationships = Set("rr_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set("r")
    )
    val `(me:START) [(a)-[r]->(b)]{0,2} (anon)` = TrailParameters(
      min = 0,
      max = Limited(2),
      start = "me",
      end = "anon",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
      groupRelationships = Set(("r_inner", "r")),
      innerRelationships = Set("r_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("me", "you", "a", "b", "r", "c", "d", "rr")
      .trail(`(anon) [(c)-[rr]->(d)]{0,2} (you)`)
      .|.expandAll("(c_inner)-[rr_inner]->(d_inner)")
      .|.argument("me", "anon", "c_inner")
      .trail(`(me:START) [(a)-[r]->(b)]{0,2} (anon)`)
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()

    // when
    val runtimeResult = execute(logicalQuery, runtime)

    // 0: (n1)
    // 1: (n1) → (n2)
    // 2: (n1) → (n2) → (n3)

    // 0: (n1)
    // 0: (n1) → (n2)
    // 0: (n1) → (n2) → (n3)
    // 1: (n1) → (n2)
    // 1: (n1) → (n2) → (n3)
    // 1: (n1) → (n2) → (n3) → (n1)
    // 2: (n1) → (n2) → (n3)
    // 2: (n1) → (n2) → (n3) → (n1)

    // then
    runtimeResult should beColumns("me", "you", "a", "b", "r", "c", "d", "rr").withRows(
      Seq(
        Array(n1, n1, emptyList(), emptyList(), emptyList(), emptyList(), emptyList(), emptyList()),
        Array(n1, n2, emptyList(), emptyList(), emptyList(), listOf(n1), listOf(n2), listOf(r12)),
        Array(n1, n3, emptyList(), emptyList(), emptyList(), listOf(n1, n2), listOf(n2, n3), listOf(r12, r23)),
        Array(n1, n2, listOf(n1), listOf(n2), listOf(r12), emptyList(), emptyList(), emptyList()),
        Array(n1, n3, listOf(n1), listOf(n2), listOf(r12), listOf(n2), listOf(n3), listOf(r23)),
        Array(n1, n1, listOf(n1), listOf(n2), listOf(r12), listOf(n2, n3), listOf(n3, n1), listOf(r23, r31)),
        Array(n1, n3, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), emptyList(), emptyList(), emptyList()),
        Array(n1, n1, listOf(n1, n2), listOf(n2, n3), listOf(r12, r23), listOf(n3), listOf(n1), listOf(r31))
      )
    )
  }

  test("should respect relationship uniqueness of previous relationships") {
    // given
    //          (n1)
    //        ↗     ↘
    //      (n3) <- (n2)
    val (n1, n2, n3, r12, r23, r31) = smallCircularGraph

    // given: MATCH (a:START)-[e]->(b) (()-[f]->(c))+
    // MATCH (a)-[e]->(b) (b)((anon_inner)-[f_inner]-(c_inner))+ (anon_end)

    val `(anon_start) (()-[f]->(c){1,*} (anon_end)` = TrailParameters(
      min = 1,
      max = Unlimited,
      start = "b",
      end = "anon_end",
      innerStart = "anon_inner",
      innerEnd = "c_inner",
      groupNodes = Set(("c_inner", "c")),
      groupRelationships = Set(("f_inner", "f")),
      innerRelationships = Set("f_inner"),
      previouslyBoundRelationships = Set("e"),
      previouslyBoundRelationshipGroups = Set.empty
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "e", "b", "f", "c")
      .trail(`(anon_start) (()-[f]->(c){1,*} (anon_end)`)
      .|.expandAll("(anon_inner)-[f_inner]->(c_inner)")
      .|.argument("anon_inner")
      .filter("a:START")
      .allRelationshipsScan("(a)-[e]->(b)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a", "e", "b", "f", "c").withRows(
      Seq(
        Array(n1, r12, n2, listOf(r23), listOf(n3)),
        Array(n1, r12, n2, listOf(r23, r31), listOf(n3, n1))
      )
    )
  }

  test("should produce rows with nullable slots") {

    // given: MATCH (a) OPTIONAL MATCH (a) ((n)-[]->(m))* (b:User) RETURN *

    val (n1, n2, n3, n4, _, _, _) = smallChainGraph
    val `(a) ((n)-[]->(m))* (b)` = TrailParameters(
      min = 0,
      max = Unlimited,
      start = "a",
      end = "b",
      innerStart = "a_inner",
      innerEnd = "b_inner",
      groupNodes = Set(("a_inner", "n"), ("b_inner", "m")),
      groupRelationships = Set(("r_anon_inner", "r_anon")),
      innerRelationships = Set("r_anon_inner"),
      previouslyBoundRelationships = Set.empty,
      previouslyBoundRelationshipGroups = Set.empty
    )
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "m", "n")
      .apply()
      .|.optional("a")
      .|.filter("b:User")
      .|.trail(`(a) ((n)-[]->(m))* (b)`)
      .|.|.expandAll("(a_inner)-[r_anon_inner]->(b_inner)")
      .|.|.argument("a_inner")
      .|.argument("a")
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("a", "b", "m", "n").withRows(
      Seq(
        Array(n1, null, null, null),
        Array(n2, null, null, null),
        Array(n3, null, null, null),
        Array(n4, null, null, null)
      )
    )
  }

  private def listOf(values: AnyRef*) = java.util.List.of(values: _*)

  // (n1) → (n2) → (n3) → (n4)
  private def smallChainGraph: (Node, Node, Node, Node, Relationship, Relationship, Relationship) = {
    given {
      val chain = chainGraphs(1, "R", "R", "R").head
      (
        chain.nodeAt(0),
        chain.nodeAt(1),
        chain.nodeAt(2),
        chain.nodeAt(3),
        chain.relationshipAt(0),
        chain.relationshipAt(1),
        chain.relationshipAt(2)
      )
    }
  }

  //          (n1)
  //        ↗     ↘
  //      (n3) <- (n2)
  private def smallCircularGraph: (Node, Node, Node, Relationship, Relationship, Relationship) = {
    given {
      val n1 = tx.createNode(label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val r12 = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
      val r31 = n3.createRelationshipTo(n1, RelationshipType.withName("R"))
      (n1, n2, n3, r12, r23, r31)
    }
  }
}
