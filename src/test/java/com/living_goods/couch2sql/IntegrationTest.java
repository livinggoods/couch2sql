package com.living_goods.couch2sql;

import java.lang.Thread;
import org.junit.Test;
import org.junit.ClassRule;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.HostPortWaitStrategy;
import org.testcontainers.containers.wait.Wait;
import org.lightcouch.CouchDbClient;
import org.lightcouch.Response;
import com.google.gson.JsonObject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/* Integration test — essentially a "unit test" for the CouchToSql
 * (main) class. */
public class IntegrationTest {
    
    @ClassRule
    public static CustomMSSQLServerContainer mssqlserver =
        new CustomMSSQLServerContainer();

    @ClassRule
    public static GenericContainer couchContainer =
        new CustomCouchDBContainer();

    @Test(timeout=20000)
    public void testCouchToSql() throws InterruptedException, SQLException {
        /* Run CouchToSql in a thread. */
        CouchToSql instance = new CouchToSql();
        Thread t = new Thread(instance);
        t.start();
        
        /* Push stuff to CouchDB. */
        CouchDbClient client = new CouchDbClient();

        /* FIXME: Push other stuff here, including some deletions. */
        
        JsonObject o = new JsonObject();
        o.addProperty("_id", "IntegrationTest");
        o.addProperty("type", "person");
        o.addProperty("name", "Sentinel for end of data processing.");
        client.save(o);

        /* Run until we see the sentinel in SQL Server. */
        Connection connection = mssqlserver.getConnection();
        PreparedStatement statement = connection.prepareStatement
            ("SELECT COUNT(*) FROM COUCHDB_IDS WHERE ID = 'IntegrationTest';");

        boolean pipeComplete = false;
        while (t.isAlive() && !pipeComplete) {
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            if (rs.getInt(1) != 0) {
                pipeComplete = true;
            }
            assertFalse(rs.next());
        }

        /* Stop the service. */
        assertTrue(t.isAlive());
        instance.stop();
        t.join();

        /* Verify database contents. */
    }
    
}
