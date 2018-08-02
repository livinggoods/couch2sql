package com.living_goods.couch2sql;

import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.wait.HostPortWaitStrategy;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertSame;

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
    
    @BeforeClass
    public static void beforeClass() throws SQLException {
        /* Create the database */
        Connection connection = mssqlserver.createConnection("");
        Statement statement = connection.createStatement();
        statement.execute("CREATE DATABASE JUNIT;");
        /* Connection should be on autocommit, but why not? */
        connection.commit();
        
        SchemaWriter sw = new SchemaWriter();
        sw.writeSchema();
    }
    
    @Test
    public void testConstructor()
    {
        SqlWriter sw = new SqlWriter();
    }
}
