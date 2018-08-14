package com.living_goods.couch2sql;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.regex.Pattern;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;

/* Class to initialize the database schema. Mainly used to set up
 * container databases for testing, but can also be run on the command
 * line. */

public class SchemaWriter {
    private static final Logger logger = LogManager.getLogger();
    private Connection connection;

    SchemaWriter() {
        connection = SqlConnection.getSqlConnection();
    }

    public void writeSchema() throws SQLException {
        final Reflections reflections = new Reflections("sqlschema",
                                                  new ResourcesScanner());
        /* Reflections gives us a HashSet, but we need the result to
         * be sorted so that we load the schema objects in order. */
        final Set<String> schemata = new TreeSet(
            reflections.getResources(Pattern.compile(".*\\.sql")));

        if (schemata.isEmpty()) {
            logger.warn("Found no schema resources to load.");
        }

        for (String resourceName : schemata) {
            processResource(resourceName);
        }
        connection.commit();
    }

    private void processResource(final String resourceName)
        throws SQLException {
        logger.info("Processing resource " + resourceName);
        final InputStream is =
            getClass().getResourceAsStream("/" + resourceName);
        final Scanner sc = new Scanner(is);
        sc.useDelimiter("\nGO\n");
        while (sc.hasNext()) {
            processBatch(sc.next());
        }
    }

    private void processBatch(final String query) throws SQLException {
        logger.debug("Processing batch:\n" + query);
        final Statement s = connection.createStatement();
        /* Stole this approach from
         * https://blog.jooq.org/2017/07/13/how-i-incorrectly-fetched-jdbc-resultsets-again/ */

        fetchLoop:
        for (int i = 0, updateCount = 0; i < 256; i++) {
            boolean result = (i == 0)
                ? s.execute(query)
                : s.getMoreResults();

            // Warnings here
            SQLWarning w = s.getWarnings();
            for (int j = 0; j < 255 && w != null; j++) {
                logger.warn(w.getMessage());
                w = w.getNextWarning();
            }

            // Don't forget this
            s.clearWarnings();

            if (result) {
                try (ResultSet rs = s.getResultSet()) {
                    logger.debug("Result      :");

                    while (rs.next()) {
                        logger.debug("  " + rs.getString(1));
                    }
                }
            } else if ((updateCount = s.getUpdateCount()) != -1) {
                logger.debug("Update Count: " + updateCount);
            } else {
                break fetchLoop;
            }
        }
    }

    public void close() throws SQLException {
        connection.close();
        connection = null;
    }

    public static void main(final String[] argv) throws SQLException {
        final SchemaWriter sw = new SchemaWriter();
        sw.writeSchema();
    }
}
