package com.living_goods.couch2sql;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.Wait;
import org.lightcouch.CouchDbClient;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;
import org.junit.ClassRule;
import org.junit.BeforeClass;
import java.lang.Thread;
import java.util.ArrayList;

/**
 * Unit test for CouchReader.
 */
public class CouchReaderTest {
    private static final String DOCKER_CONTAINER_NAME = "couchdb:1.6.1";

    @ClassRule
    public static GenericContainer couchContainer =
        new FixedHostPortGenericContainer(DOCKER_CONTAINER_NAME)
        .withFixedExposedPort(5985, 5984)
        .waitingFor(Wait.forHttp("/")
                    .forStatusCode(200));

    private static ArrayList<JsonElement> testData;

    @BeforeClass
    public static void beforeClass() {
        /* Come up with some test data & stuff it in the DB. */
        final int TEST_SIZE = 450; /* At least a few pages */
        testData = new ArrayList<JsonElement>(TEST_SIZE);
        for (int i = 0; i < TEST_SIZE; i ++) {
            JsonObject o = new JsonObject();
            o.addProperty("_id", String.valueOf(i));
            o.addProperty("ordinal", i);
            testData.add(o);
        }
        CouchDbClient client = new CouchDbClient();
        for (JsonElement o : testData) {
            client.save(o);
        }
    }

    @Test
    public void testConstructor()
    {
        PipedMock p = new PipedMock<JsonObject>();
        CouchReader cr = new CouchReader(p);
        assertTrue(p.getSent().isEmpty());
    }

    @Test
    public void testSetSeq()
    {
        Piped p = new PipedMock<JsonObject>();
        CouchReader cr = new CouchReader(p);
        cr.setSeq("");
    }

    @Test(timeout=20000)
    public void checkData() {
        PipedMock p = new PipedMock<JsonObject>();
        CouchReader cr = new CouchReader(p);
        Thread t = new Thread(() -> cr.run());
        t.start();
        /* Run until we get back all 200 items from the pipe. */
        while (t.isAlive() && p.getSent().size() < testData.size()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {}
            System.out.println("Got " + p.getSent().size() + " docs");
        }
        assertTrue(t.isAlive());
        cr.stop();
        try {
            t.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        ArrayList<JsonObject> sent = p.getSent();
        for (int i = 0; i < testData.size(); i ++) {
            JsonObject fromCouch = sent.get(i);
            assertTrue(fromCouch.has("_rev"));
            fromCouch.remove("_rev");
        }
        assertArrayEquals(testData.toArray(), sent.toArray());
    }
}
