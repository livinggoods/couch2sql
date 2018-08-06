package com.living_goods.couch2sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections4.map.MultiKeyMap;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.ListIterator;
import java.util.stream.Collectors;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lightcouch.ChangesResult.Row;

/* Class which updates SQL Server with the resulting data. */
public class SqlWriter implements Piped<TransformedChange> {
    private Connection connection;
    private String seq;
    private PreparedStatement getSeqStatement;
    private PreparedStatement updateSeqStatement;
    private PreparedStatement insertSeqStatement;
    private PreparedStatement insertCouchIdStatement;
    private PreparedStatement deleteCouchIdStatement;
    private MultiKeyMap deleteStmtCache;
    private MultiKeyMap insertStmtCache;
    private static final Logger logger = LogManager.getLogger();

    public static final String DATA_SOURCE_CONTEXT = "sqldb";
    
    SqlWriter() {
        logger.debug("SqlWriter constructor");
        connection = SqlConnection.getSqlConnection();
        try {
            getSeqStatement = connection.prepareStatement
                ("SELECT LAST_SEQ FROM COUCHDB_REPLICATION;");
            updateSeqStatement = connection.prepareStatement
                ("UPDATE COUCHDB_REPLICATION SET LAST_SEQ = ? " +
                 "OUTPUT deleted.LAST_SEQ;");
            insertSeqStatement = connection.prepareStatement
                ("INSERT INTO COUCHDB_REPLICATION (LAST_SEQ) VALUES (?);");
            insertCouchIdStatement = connection.prepareStatement
                ("INSERT INTO COUCHDB_IDS (ID, \"TABLE\") VALUES (?, (" +
                 "  SELECT CT.ID FROM COUCHDB_TABLES CT " +
                 "  WHERE CT.TABLE_NAME = ?));");
            deleteCouchIdStatement = connection.prepareStatement
                ("DELETE CI " +
                 "OUTPUT CT.TABLE_NAME " +
                 "FROM COUCHDB_TABLES CT, COUCHDB_IDS CI " +
                 "WHERE CT.ID = CI.\"TABLE\" AND CI.ID = ?;");
            
            seq = getSeqFromDB();
        } catch (SQLException e) {
            logger.fatal("Error setting up prepared statements", e);
            throw new IllegalStateException(e);
        }

        deleteStmtCache = new MultiKeyMap();
        insertStmtCache = new MultiKeyMap();
    }

    /* Looks up the current last sequence number in the DB and returns
     * it. Retuns null if there is no sequence number in the DB. */
    private String getSeqFromDB() throws SQLException {
        final ResultSet rs = getSeqStatement.executeQuery();
        if (!rs.next()) {
            return null;
        } else {
            return rs.getString("LAST_SEQ");
        }
    }

    /* Returns the last CouchDB sequence number seen in this database. */
    public String getSeq() {
        return seq;
    }
    
    @Override
    public void close() {
        try {
            logger.debug("SqlWriter closed");
            connection.close();
        } catch (SQLException e) {
            logger.fatal("Database error closing connection", e);
            throw new IllegalStateException(e);
        }
    }

    /* Receive a single transformed JSON document, and update it in
     * the database. */
    @Override
    public void send(TransformedChange input) {
        try {
            Row row = input.getRow();
            if (row.isDeleted()) {
                /* Handle record deletion. */
                deleteCouchId(row.getId());
            } else {
                /* Update/Insert */
                JsonNode json = input.getResult();
                /* We do this in three steps:
                 * 1. Clear out dimension table entries, if any
                 * 2. UPDATE or INSERT the target table and ID table.
                 * 3. Insert to the dimension table, if any.
                 */
                /* n.b. json.get("foobar") will return null if absent. */
                final JsonNode dimensions = json.get("dimensions");
                deleteDimensionTables(dimensions);
                upsertTargetTable(json);
                insertDimensionTables(dimensions);
            }
            updateSequence(row.getSeq());
            connection.commit();
        } catch (SQLException e) {
            logger.fatal("Database error during DML.", e);
            throw new IllegalStateException(e);
        }
    }

    private void deleteCouchId(String id) throws SQLException {
        deleteCouchIdStatement.setString(1, id);
        deleteCouchIdStatement.execute();
        ResultSet rs = deleteCouchIdStatement.getResultSet();
        if (!rs.next()) {
            /* The ID was not found. This is not as bad as it sounds,
             * as there is garbage in CouchDB that we don't load to
             * SQL Server.
             */
            return;
        }
        String tableName = rs.getString(1);
        final PreparedStatement deleteStmt =
            makeDeleteStatement(tableName, "ID", id);
        deleteStmt.executeUpdate();
        assert !rs.next();
    }
    
    /* Clear out any dimension tables referenced by the input JSON
     * document. */
    private void deleteDimensionTables(JsonNode dimensions)
        throws SQLException {
        if (dimensions == null) { return; }
        assert dimensions.isArray();
        for (JsonNode dimension : dimensions) {
            final String table = dimension.get("table").textValue();
            final String key_column = dimension.get("key_column").textValue();
            final String key_value = dimension.get("key_value").textValue();
            final PreparedStatement stmt =
                makeDeleteStatement(table, key_column, key_value);
            stmt.executeUpdate();
        }
    }

    private void insertDimensionTables(JsonNode dimensions)
        throws SQLException {
        if (dimensions == null) { return; }
        assert dimensions.isArray();
        for (JsonNode dimension : dimensions) {
            final String table = dimension.get("table").textValue();
            final String key_column = dimension.get("key_column").textValue();
            final String key_value = dimension.get("key_value").textValue();

            for (JsonNode row : dimension.get("rows")) {
                /* The row is just an object with keys/values. */
                final ObjectNode objRow = (ObjectNode) row;
                objRow.put(key_column, key_value);
                final PreparedStatement stmt =
                    makeInsertStatement(table, objRow);
                stmt.executeUpdate();
            }
        }
    }
    
    private void upsertTargetTable(JsonNode input) throws SQLException {
        /* Do DELETE followed by INSERT. I read here that it's faster
           than MERGE.
         * https://www.mssqltips.com/sqlservertip/2651/comparing-performance-for-the-merge-statement-to-select-insert-update-or-delete/ */
        final String table = input.get("table").textValue();
        final JsonNode row = input.get("row");
        final PreparedStatement deleteStmt =
            makeDeleteStatement(table, "ID", row.get("ID").textValue());
        final int deletedRows = deleteStmt.executeUpdate();
        logger.debug("Deleted " + deletedRows + " existing rows");
        if (deletedRows == 0) {
            /* This is a new row, need to insert COUCHDB_IDS too */
            insertCouchIdStatement.setString(1, row.get("ID").textValue());
            insertCouchIdStatement.setString(2, table);
            insertCouchIdStatement.executeUpdate();
        } else {
            assert deletedRows == 1;
        }

        final PreparedStatement insertStmt = makeInsertStatement(table, row);
        insertStmt.executeUpdate();
    }

    /* Update the COUCHDB_REPLICATION table with the latest sequence. */
    private void updateSequence(String seq) throws SQLException {
        updateSeqStatement.setString(1, seq);
        updateSeqStatement.execute();
        ResultSet rs = updateSeqStatement.getResultSet();
        if (!rs.next()) {
            /* Nothing was there, do insert. */
            insertSeqStatement.setString(1, seq);
            insertSeqStatement.executeUpdate();
        } else {
            String oldSeq = rs.getString(1);
            if (rs.next()) {
                throw new SQLException
                    ("COUCHDB_REPLICATION table has more than 1 row!");
            } else {
                /* Double check that the previous sequence number is the
                 * one that we updated. */
                if (! oldSeq.equals(this.seq)) {
                    throw new SQLException (String.format
                        ("COUCHDB_REPLICATION updated unexpectedly " +
                         "(concurrency issue?), saw %s " +
                         "while updating from %s to %s",
                         oldSeq, this.seq, seq));
                }
            }
        }
        this.seq = seq;
    }
    
    /* Returns a prepared statement, with parameters already
     * populated, that deletes the row from the given table where the
     * given column matches the given value. */
    private PreparedStatement makeDeleteStatement
        (String table, String column, String value)
        throws SQLException {
        PreparedStatement rval =
            (PreparedStatement) deleteStmtCache.get(table, column);
        if (rval == null) {
            rval = connection.prepareStatement
                ("DELETE FROM \"" + table + "\" WHERE \"" +
                 column + "\" = ?;");
            deleteStmtCache.put(table, column, rval);
        }

        rval.setString(1, value);
        return rval;
    }

    /* Returns a prepared statement, with parameters already
     * populated, that inserts a row into the given table according to
     * the provided values. The values parameter must have string keys
     * and value-typed values, correspodning to column names and
     * values. */
    private PreparedStatement makeInsertStatement
        (String table, JsonNode data)
        throws SQLException {
        assert data.isObject();
        final ArrayList<String> columns = new ArrayList<String>(data.size());
        final ArrayList<String> values = new ArrayList<String>(data.size());
        for (Iterator<Map.Entry<String,JsonNode>> fields = data.fields();
             fields.hasNext();) {
            final Map.Entry<String,JsonNode> map = fields.next();
            columns.add(map.getKey());
            final JsonNode value = map.getValue();
            assert value.isValueNode();
            /* I think if we cast everything to text, SQL
             * Server will parse it and figure it out. */
            values.add(value.asText());
        }

        PreparedStatement rval =
            (PreparedStatement) insertStmtCache.get(table, columns);
        if (rval == null) {
            final String columnList =
                columns.stream()
                .map(elem -> "\"" + elem + "\"")
                .collect(Collectors.joining(","));
            final String parameterList =
                columns.stream()
                .map(elem -> "?")
                .collect(Collectors.joining(","));

            String sql = ("INSERT INTO \"" + table + "\" (" +
                          columnList + ") VALUES (" + parameterList + ");");
            logger.debug("Registering query " + sql);
            rval = connection.prepareStatement(sql);
            insertStmtCache.put(table, columns, rval);
        }
        
        for (ListIterator<String> i = values.listIterator(); i.hasNext();) {
            final int ordinal = i.nextIndex() + 1;
            final String value = i.next();
            rval.setString(ordinal, value);
            logger.debug("Setting parameter " + ordinal + " = " + value);
        }
        return rval;
    }
}
