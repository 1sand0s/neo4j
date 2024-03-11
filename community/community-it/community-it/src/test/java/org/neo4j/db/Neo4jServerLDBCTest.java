package org.neo4j.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.*;

public class Neo4jServerLDBCTest {

    String uri = "bolt://localhost:7687";
    String user = "neo4j";
    String password = "passwd123";

    String[] queries;

    @Test
    public void testRandomSequence() {
        String[] index = new String[] {"A", "B", "C", "D", "E", "F", "G", "H", "I"};
        queries = new String[] {
            "CYPHER replan=force MATCH (m:`Person`"
                    + " {id:6597069786683})-[likes:LIKES]->(:`Post;`"
                    + " {id:1649267441739}) DELETE likes RETURN COUNT(m)",
            "CYPHER replan=force MATCH (m:`Person`"
                    + " {id:6597069780295})-[likes:LIKES]->(:`Comment;`"
                    + " {id:4123168604177}) DELETE likes RETURN COUNT(m)",
            "CYPHER replan=force MATCH (m:`Forum`"
                    + " {id:2748779069441})-[hasMember:HAS_MEMBER]->(:`Person;`"
                    + " {id:6597069776731}) DELETE hasMember RETURN COUNT(m)",
            "CYPHER replan=force MATCH (n:`Person`"
                    + " {id:32985348833679})-[:IS_LOCATED_IN]->(p:`Place`)"
                    + " RETURN n.firstName AS firstName, n.lastName AS lastName, n.birthday AS"
                    + " birthday, n.locationIP AS locationIP, n.browserUsed AS browserUsed, p.id AS"
                    + " cityId, n.gender AS gender, n.creationDate AS creationDate",
            "CYPHER replan=force MATCH (m:`Comment`"
                    + " {id:2199023299651})-[:HAS_CREATOR]->(p:`Person`)"
                    + " RETURN p.id AS personId, p.firstName AS firstName, p.lastName AS lastName",
            "CYPHER replan=force MATCH (person:`Person` {id:6597069786683}),"
                    + " (post:`Post` {id:1924145349113})  CREATE"
                    + " (person)-[:LIKES]->(post)",
            "CYPHER replan=force MATCH (person:`Person` {id:6597069780295}),"
                    + " (comment:`Comment` {id:2199023256180})  CREATE"
                    + " (person)-[:LIKES]->(comment)",
            "CYPHER replan=force MATCH (f:`Forum` {id:2748779069441}),"
                    + " (p:`Person` {id:19791209317730})  CREATE"
                    + " (f)-[:HAS_MEMBER]->(p)",
            "CYPHER replan=force MATCH (p1:`Person` {id:2199023256684}),"
                    + " (p2:`Person` {id:30786325587981})  CREATE"
                    + " (p1)-[:KNOWS]->(p2)"
        };
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i <= 8; i++) {
            indices.add(i);
        }
        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))) {
            // Create a session
            try (Session session = driver.session()) {
                Collections.shuffle(indices);
                long t1 = System.nanoTime();
                for (int j = 0; j < indices.size(); j++) {
                    long t2 = System.nanoTime();
                    session.run(queries[indices.get(j)]);
                    System.out.println("Time for Query " + index[indices.get(j)] + " : " + (System.nanoTime() - t2));
                }
                System.out.println("Total Time : " + (System.nanoTime() - t1));
            }
        }
    }

    @Test
    public void testNodeCount() {
        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))) {
            // Create a session
            try (Session session = driver.session()) {

                Result result = session.run("MATCH (n) RETURN COUNT(n)");
                while (result.hasNext()) {
                    org.neo4j.driver.Record record = result.next();

                    System.out.println("Nodes " + record.get("COUNT(n)"));
                }
            }
        }
    }

    @Test
    public void testRelationCount() {
        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))) {
            // Create a session
            try (Session session = driver.session()) {

                Result result = session.run("MATCH (n)-[r]->(m) RETURN COUNT(n)");
                while (result.hasNext()) {
                    org.neo4j.driver.Record record = result.next();

                    System.out.println("Relations " + record.get("COUNT(n)"));
                }
            }
        }
    }

    @Test
    public void testDataNodes() {
        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))) {
            // Create a session
            try (Session session = driver.session()) {

                Result result = session.run("MATCH (n) RETURN labels(n), COUNT(labels(n))");
                while (result.hasNext()) {
                    org.neo4j.driver.Record record = result.next();

                    System.out.println("Nodes " + record);
                }
            }
        }
    }

    @Test
    public void testDataRelations() {
        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))) {
            // Create a session
            try (Session session = driver.session()) {

                Result result =
                        session.run("MATCH (n)-[r]->(m) RETURN labels(n), type(r), labels(m) , COUNT(labels(n))");
                while (result.hasNext()) {
                    org.neo4j.driver.Record record = result.next();

                    System.out.println("Relations " + record);
                }
            }
        }
    }

    @Test
    public void testInteractiveDeleteQuery2() {
        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))) {
            // Create a session
            try (Session session = driver.session()) {
                long t1 = System.nanoTime();
                session.run("MATCH (m:`Person`"
                        + " {id:6597069786683})-[likes:LIKES]->(:`Post;`"
                        + " {id:1649267441739}) DELETE likes RETURN COUNT(m)");
                System.out.println("Total Query Time : " + (System.nanoTime() - t1));
            }
        }
    }

    //
    @Test
    public void testInteractiveDeleteQuery3() {
        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))) {
            // Create a session
            try (Session session = driver.session()) {
                long t1 = System.nanoTime();
                session.run("MATCH (m:`Person`"
                        + " {id:6597069780295})-[likes:LIKES]->(:`Comment;`"
                        + " {id:4123168604177}) DELETE likes RETURN COUNT(m)");
                System.out.println("Total Query Time : " + (System.nanoTime() - t1));
            }
        }
    }

    //
    @Test
    public void testInteractiveDeleteQuery5() {
        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))) {
            // Create a session
            try (Session session = driver.session()) {
                long t1 = System.nanoTime();
                session.run("MATCH (m:`Forum`"
                        + " {id:2748779069441})-[hasMember:HAS_MEMBER]->(:`Person;`"
                        + " {id:6597069776731}) DELETE hasMember RETURN COUNT(m)");
                System.out.println("Total Query Time : " + (System.nanoTime() - t1));
            }
        }
    }

    //
    @Test
    public void testInteractiveShortQuery1() {
        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))) {
            // Create a session
            try (Session session = driver.session()) {
                long t1 = System.nanoTime();
                Result result = session.run("MATCH (n:`Person`"
                        + " {id:32985348833679})-[:IS_LOCATED_IN]->(p:`Place`)"
                        + " RETURN n.firstName AS firstName, n.lastName AS lastName, n.birthday AS"
                        + " birthday, n.locationIP AS locationIP, n.browserUsed AS browserUsed, p.id AS"
                        + " cityId, n.gender AS gender, n.creationDate AS creationDate");
                System.out.println("Total Query Time : " + (System.nanoTime() - t1));
                while (result.hasNext()) {
                    org.neo4j.driver.Record record = result.next();

                    System.out.println("Result " + record);
                }
            }
        }
    }

    //
    @Test
    public void testInteractiveUpdateQuery2() {
        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))) {
            // Create a session
            try (Session session = driver.session()) {
                long t1 = System.nanoTime();
                session.run("MATCH (person:`Person` {id:6597069786683}),"
                        + " (post:`Post` {id:1924145349113})  CREATE"
                        + " (person)-[:LIKES]->(post)");
                System.out.println("Total Query Time : " + (System.nanoTime() - t1));
            }
        }
    }

    //
    @Test
    public void testInteractiveUpdateQuery3() {
        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))) {
            // Create a session
            try (Session session = driver.session()) {
                long t1 = System.nanoTime();
                session.run("MATCH (person:`Person` {id:6597069780295}),"
                        + " (comment:`Comment` {id:2199023256180})  CREATE"
                        + " (person)-[:LIKES]->(comment)");
                System.out.println("Total Query Time : " + (System.nanoTime() - t1));
            }
        }
    }

    //
    @Test
    public void testInteractiveUpdateQuery5() {
        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))) {
            // Create a session
            try (Session session = driver.session()) {
                long t1 = System.nanoTime();
                session.run("MATCH (f:`Forum` {id:2748779069441}),"
                        + " (p:`Person` {id:19791209317730})  CREATE"
                        + " (f)-[:HAS_MEMBER]->(p)");
                System.out.println("Total Query Time : " + (System.nanoTime() - t1));
            }
        }
    }

    //
    @Test
    public void testInteractiveUpdateQuery8() {
        try (Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))) {
            // Create a session
            try (Session session = driver.session()) {
                long t1 = System.nanoTime();
                session.run("MATCH (p1:`Person` {id:2199023256684}),"
                        + " (p2:`Person` {id:30786325587981})  CREATE"
                        + " (p1)-[:KNOWS]->(p2)");
                System.out.println("Total Query Time : " + (System.nanoTime() - t1));
            }
        }
    }
}
