package com.living_goods.couch2sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections4.map.MultiKeyMap;
import java.io.InputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.ListIterator;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.osjava.sj.loader.convert.SJDataSourceConverter;

/* Class which updates SQL Server with the resulting data. */
public class SqlWriter implements Piped<TransformedChange> {
    private Connection connection;
    private String seq;
    private PreparedStatement getSeqStatement;
    private PreparedStatement updateSeqStatement;
    private PreparedStatement insertSeqStatement;
    private MultiKeyMap deleteStmtCache;
    private MultiKeyMap insertStmtCache;
    private static final Logger logger = LogManager.getLogger();

    public static final String DATA_SOURCE_CONTEXT = "sqldb";
    
    SqlWriter() {
        /* Load the SQL database connection.

           This is a bit of a hack. The right way is to use JNDI to
           get a DataSource, but I couldn't find a JNDI implementation
           that I liked (wanted to load the data source properties
           from the classpath). Instead we find the properties file
           ourselves, and then use Simple-JNDI to convert the
           properties file into a DataSource.

           I have opened an enhancement request for Simple-JNDI to
           support loading properties from the classpath, after which
           the lookup could be done simply with
           InitialContext.lookup().
           https://github.com/h-thurow/Simple-JNDI/issues/9
        */
        try {
            try (InputStream configStream =
                 getClass().getResourceAsStream("/" + DATA_SOURCE_CONTEXT +
                                                ".properties")) {
                final Properties properties = new Properties();
                properties.load(configStream);
                final SJDataSourceConverter loader =
                    new SJDataSourceConverter();
                final DataSource ds = (DataSource) 
                    loader.convert(properties,
                                   properties.getProperty("type"));
                connection = ds.getConnection();
            } catch (IOException e) {
                String msg = "Could not read SQL Server configuration from the classpath: "
                    + DATA_SOURCE_CONTEXT;
                logger.fatal(msg, e);
                throw new IllegalStateException(msg, e);
            }

            connection.setAutoCommit(false);
            final int tiso = Connection.TRANSACTION_SERIALIZABLE;
            connection.setTransactionIsolation(tiso);

            getSeqStatement = connection.prepareStatement
                ("SELECT LAST_SEQ FROM COUCHDB_REPLICATION;");
            updateSeqStatement = connection.prepareStatement
                ("UPDATE COUCHDB_REPLICATION SET LAST_SEQ = ?;");
            insertSeqStatement = connection.prepareStatement
                ("INSERT INTO COUCHDB_REPLICATION (LAST_SEQ) VALUES (?);");

            seq = getSeq();
        } catch (SQLException e) {
            logger.fatal("Error setting up SQL Server connection", e);
            throw new IllegalStateException(e);
        }

        deleteStmtCache = new MultiKeyMap();
        insertStmtCache = new MultiKeyMap();
    }

    /* Looks up the current last sequence number in the DB and returns it. */
    private String getSeq() throws SQLException {
        final ResultSet rs = getSeqStatement.executeQuery();
        return rs.getString("LAST_SEQ");
    }
    
    @Override
    public void close() {
        try {
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
        JsonNode json = input.getResult();
        try {
            /* We do this in three steps:
             * 1. Clear out dimension table entries, if any
             * 2. UPDATE or INSERT the target table.
             * 3. Insert to the dimension table, if any.
             */
            final JsonNode dimensions = json.get("dimensions");
            deleteDimensionTables(dimensions);
            upsertTargetTable(json);
            insertDimensionTables(dimensions);
            updateSequence(input.getRow().getSeq());
            connection.commit();
        } catch (SQLException e) {
            logger.fatal("Database error writing/updating data.", e);
            throw new IllegalStateException(e);
        }
    }

    /* Clear out any dimension tables referenced by the input JSON
     * document. */
    private void deleteDimensionTables(JsonNode dimensions)
        throws SQLException {
        assert dimensions.isArray();
        for (JsonNode dimension : dimensions) {
            final String table = dimension.get("table").textValue();
            final String key_column = dimension.get("key_column").textValue();
            final String key_value = dimension.get("key_value").textValue();
            final PreparedStatement stmt =
                makeDeleteStatement(table, key_column, key_value);
            stmt.execute();
        }
    }

    private void insertDimensionTables(JsonNode dimensions)
        throws SQLException {
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
                stmt.execute();
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
        deleteStmt.execute();
        final PreparedStatement insertStmt = makeInsertStatement(table, row);
        insertStmt.execute();
    }

    /* Update the COUCHDB_REPLICATION table with the latest sequence. */
    private void updateSequence(String seq) throws SQLException {
        updateSeqStatement.setString(1, seq);
        int rows = updateSeqStatement.executeUpdate();
        if (rows > 1) {
            throw new SQLException
                ("COUCHDB_REPLICATION table has more than 1 row!");
        } else if (rows == 0) {
            return;
        }
        /* rows == 1, do insert. */
        insertSeqStatement.setString(1, seq);
        insertSeqStatement.execute();
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
            
            rval = connection.prepareStatement
                ("INSERT INTO \"" + table + "\" (" +
                 columnList + ") VALUES (" + parameterList + ");");
            insertStmtCache.put(table, columns, rval);
        }
        
        for (ListIterator<String> i = values.listIterator(); i.hasNext();) {
            final int ordinal = i.nextIndex() + 1;
            final String value = i.next();
            rval.setString(ordinal, value);
        }
        return rval;
    }
}
