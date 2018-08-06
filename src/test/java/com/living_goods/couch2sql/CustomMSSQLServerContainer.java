package com.living_goods.couch2sql;

import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.ContainerLaunchException;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;

/* We customize the MSSQLServerContainer to get a fixed host port,
 * so that we can test configuration using properties. */
class CustomMSSQLServerContainer
    extends MSSQLServerContainer {
    public CustomMSSQLServerContainer() {
        super();
        this.addFixedExposedPort(1434, 1433);
    }

    /* This is only used by the tests themselves, to set things up out
     * of band. */
    public Connection getConnection() throws SQLException {
        Connection rval =
            createConnection(";databaseName=JUNIT");
        rval.setAutoCommit(true);
        return rval;
    }

    @Override
    protected void waitUntilContainerStarted() {
        super.waitUntilContainerStarted();

        try {
            /* Create the database */
            try (Connection connection = createConnection("")) {
                Statement statement = connection.createStatement();
                statement.execute("CREATE DATABASE JUNIT;");
                /* Connection should be on autocommit, but just in case... */
                connection.commit();
            }
            
            SchemaWriter sw = new SchemaWriter();
            sw.writeSchema();
            sw.close();
        } catch (SQLException e) {
            throw new ContainerLaunchException("Error setting up metadata", e);
        }
    }
}

