package com.living_goods.couch2sql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.lightcouch.ChangesResult.Row;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.wait.HostPortWaitStrategy;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Set;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import static org.assertj.core.api.Assertions.assertThat;

/* Unit test for SqlWriter */
public class SqlWriterTest {
    /* We customize the MSSQLServerContainer to get a fixed host port,
     * so that we can test configuration using JNDI. */
    private static class CustomMSSQLServerContainer
        extends MSSQLServerContainer {
        public CustomMSSQLServerContainer() {
            super();
            this.addFixedExposedPort(1434, 1433);
        }
    }

    @ClassRule
    public static MSSQLServerContainer mssqlserver =
        new CustomMSSQLServerContainer();
    
    private synchronized Connection getConnection() throws SQLException {
        Connection rval = mssqlserver.createConnection(";databaseName=JUNIT");
        rval.setAutoCommit(true);
        return rval;
    }
    
    @BeforeClass
    public synchronized static void beforeClass() throws SQLException {
        /* Create the database */
        try (Connection connection = mssqlserver.createConnection("")) {
            Statement statement = connection.createStatement();
            statement.execute("CREATE DATABASE JUNIT;");
            /* Connection should be on autocommit, but just in case... */
            connection.commit();
        }
        
        SchemaWriter sw = new SchemaWriter();
        sw.writeSchema();
        sw.close();
    }
    
    private static final Logger logger = LogManager.getLogger();

    @Test
    public synchronized void testConstructor() {
        /* This seems like a simple test but it actually validates
         * quite a lot, including the ability to connect to the
         * database, check the sequence number, and set up the
         * prepared statements. */
        logger.debug("Entering testConstructor()");
        try (SqlWriter sw = new SqlWriter()) {}
        logger.debug("Exiting testConstructor()");
    }

    @Test
    public synchronized void testGetSeq() throws SQLException {
        logger.debug("Entering testGetSeq()");
        String seq = "abcd1234deadbeef";
        try (Connection connection = getConnection()) {
            /* In case we are run after other tests. */
            Statement deleteStatement = connection.createStatement();
            deleteStatement.executeUpdate
                ("DELETE FROM COUCHDB_REPLICATION;");
            PreparedStatement insertStatement = connection.prepareStatement
                ("INSERT INTO COUCHDB_REPLICATION (LAST_SEQ) VALUES (?);");
            insertStatement.setString(1, seq);
            insertStatement.execute();
        }
        
        try (SqlWriter sw = new SqlWriter()) {
            assertEquals(seq, sw.getSeq());
        }
        logger.debug("Exiting testGetSeq()");
    }

    /* Runs a bunch of asserts to compare the values in the ResultSet
     * to the matching ObjectNode. */
    private void compareJsonToRow(ResultSet result, ObjectNode json)
        throws SQLException {
        ResultSetMetaData meta = result.getMetaData();
        Set<String> columns = new HashSet<String>();;
        for (int i = 1; i <= meta.getColumnCount(); i ++) {
            String column = meta.getColumnName(i);
            assertNotNull(column);
            /* If we got a NULL value of of the database, ignore it in
             * JSON. */
            if (result.getObject(i) != null) {
                JsonNode jsonValue = json.get(column);
                assertNotNull("JSON Object does not have column " + column,
                              jsonValue);
                columns.add(column);
                switch (meta.getColumnType(i)) {
                case java.sql.Types.BOOLEAN:
                case java.sql.Types.BIT:
                    assertEquals(result.getBoolean(i),
                                 jsonValue.booleanValue());
                    break;
                case java.sql.Types.INTEGER:
                case java.sql.Types.SMALLINT:
                case java.sql.Types.TINYINT:
                    assertEquals(result.getInt(i), jsonValue.intValue());
                    break;
                case java.sql.Types.DOUBLE:
                case java.sql.Types.FLOAT:
                    assertEquals(result.getFloat(i), jsonValue.floatValue(),
                                 0.00001);
                    break;
                case java.sql.Types.NVARCHAR:
                case java.sql.Types.TIMESTAMP:
                    assertEquals(result.getString(i), jsonValue.textValue());
                    break;
                default:
                    fail("Got unexpected SQL type " + meta.getColumnType(i));
                }
            }
        }
        assertThat(json.fieldNames())
            .containsExactlyInAnyOrderElementsOf(columns);
    }
    
    @Test
    public synchronized void testInsert() throws SQLException {
        logger.debug("Entering testInsert()");
        /* Pass a record in, and check out the resulting database state. */
        ObjectNode doc = new ObjectNode(JsonNodeFactory.instance);
        doc.put("table", "CLINIC");
        ObjectNode row = new ObjectNode(JsonNodeFactory.instance);
        row.put("ID", "abc");
        row.put("NAME", "Turner Family");
        row.put("SOLAR_LIGHT", false);
        row.put("WATER_FILTER", false);
        row.put("CHILDREN_UNDER_5", 0);
        row.put("IMPROVED_COOK_STOVE", true);
        row.put("LLIN", 0);
        row.put("LATRINE", true);
        row.put("HOW_WATER_TREATED", "Chlorine");
        row.put("HAND_WASHING_FACILITIES", true);
        row.put("LATITUDE", 40.75);
        row.put("LONGITUDE", -73.866667);
        row.put("IMPORTED_DATE", "2018-01-01 12:00:01.0");
        row.put("REPORTED_DATE", "2018-01-01 12:00:02.0");
        doc.set("row", row);
        Row couchRow = new RowMock("1234abcd", null);
        TransformedChange tc = new TransformedChange(couchRow, doc);
        try (SqlWriter sw = new SqlWriter()) {
            sw.send(tc);
            assertEquals(sw.getSeq(), "1234abcd");
        }

        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM CLINIC");
        assertTrue(rs.next());
        compareJsonToRow(rs, row);
        assertFalse(rs.next());
        logger.debug("Exiting testInsert()");
    }

    @Test
    public synchronized void testUpdate() throws SQLException {
        logger.debug("Exiting testUpdate()");
        /* Send an update to a record, then check the
         * database. */
        ObjectNode doc = new ObjectNode(JsonNodeFactory.instance);
        doc.put("table", "HEALTH_CENTER");
        ObjectNode row = new ObjectNode(JsonNodeFactory.instance);
        row.put("ID", "abc");
        row.put("NAME", "Kathleen Berg");
        row.put("IMPORTED_DATE", "2018-01-01 12:00:01.0");
        row.put("REPORTED_DATE", "2018-01-01 12:00:02.0");
        doc.set("row", row);
        Row couchRow = new RowMock("1", null);
        TransformedChange tc = new TransformedChange(couchRow, doc);
        try (SqlWriter sw = new SqlWriter()) {
            sw.send(tc);

            row.put("NAME", "Penny Leon");
            row.put("REPORTED_DATE", "2018-08-01 12:00:02.0");
            couchRow = new RowMock("2", null);
            tc = new TransformedChange(couchRow, doc);
            sw.send(tc);
            assertEquals(sw.getSeq(), "2");
        }

        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM HEALTH_CENTER");
        assertTrue(rs.next());
        compareJsonToRow(rs, row);
        assertFalse(rs.next());
        logger.debug("Exiting testUpdate()");
    }

    private ObjectNode dimensionDoc(String id, String... values) {
        ObjectNode doc = new ObjectNode(JsonNodeFactory.instance);
        doc.put("table", "PERSON");
        ObjectNode row = new ObjectNode(JsonNodeFactory.instance);
        row.put("ID", id);
        row.put("REV", "Should be a UUID");
        row.put("NAME", "Dean Contreras");
        doc.set("row", row);
        ArrayNode dimensions = new ArrayNode(JsonNodeFactory.instance);
        ObjectNode dimension = new ObjectNode(JsonNodeFactory.instance);
        dimension.put("table", "PERSON_FP_RISK_FACTORS");
        dimension.put("key_column", "PERSON_ID");
        dimension.put("key_value", id);
        ArrayNode dimensionRows = new ArrayNode(JsonNodeFactory.instance);
        for (String v : values) {
            ObjectNode dimensionRow = new ObjectNode(JsonNodeFactory.instance);
            dimensionRow.put("RISK_FACTOR", v);
            dimensionRows.add(dimensionRow);
        }
        dimension.put("rows", dimensionRows);
        dimensions.add(dimension);
        doc.put("dimensions", dimensions);
        return doc;
    }
    
    @Test
    public synchronized void testDimensionInsert() throws SQLException {
        logger.debug("Entering testDimensionInsert()");
        /* Pass a record in, and check out the resulting database state. */
        ObjectNode doc = dimensionDoc("tdi", "cute", "partners");
        Row couchRow = new RowMock("tdi", null);
        TransformedChange tc = new TransformedChange(couchRow, doc);
        try (SqlWriter sw = new SqlWriter()) {
            sw.send(tc);
            assertEquals(sw.getSeq(), "tdi");
        }

        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery
            ("SELECT * FROM PERSON_FP_RISK_FACTORS");
        Set<String> sqlFactors = new HashSet<String>();
        assertTrue(rs.next());
        assertEquals(rs.getString("PERSON_ID"), "tdi");
        sqlFactors.add(rs.getString("RISK_FACTOR"));
        assertTrue(rs.next());
        assertEquals(rs.getString("PERSON_ID"), "tdi");
        sqlFactors.add(rs.getString("RISK_FACTOR"));
        assertFalse(rs.next());
        assertThat(sqlFactors)
            .containsExactlyInAnyOrder("cute", "partners");
                       
        logger.debug("Exiting testDimensionInsert()");
    }

    @Test
    public synchronized void testDimensionDelete() throws SQLException {
        logger.debug("Entering testDimensionInsert()");
        /* Pass a record in, and check out the resulting database state. */
        try (SqlWriter sw = new SqlWriter();
             Connection connection = getConnection();
             ) {
            ObjectNode doc = dimensionDoc("tdd", "cute", "partners");
            Row couchRow = new RowMock("tdd1", null);
            TransformedChange tc = new TransformedChange(couchRow, doc);
            sw.send(tc);
            assertEquals(sw.getSeq(), "tdd1");
            
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery
                ("SELECT COUNT(*) FROM PERSON_FP_RISK_FACTORS " +
                 "WHERE PERSON_ID = 'tdd';");
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 2);
            assertFalse(rs.next());
            
            doc = dimensionDoc("tdd");
            couchRow = new RowMock("tdd2", null);
            tc = new TransformedChange(couchRow, doc);
            sw.send(tc);
            assertEquals(sw.getSeq(), "tdd2");

            rs = statement.executeQuery
                ("SELECT COUNT(*) FROM PERSON_FP_RISK_FACTORS " +
                 "WHERE PERSON_ID = 'tdd';");
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 0);
            assertFalse(rs.next());
        }

        logger.debug("Exiting testDimensionInsert()");
    }
    
}
