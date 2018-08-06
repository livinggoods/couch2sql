package com.living_goods.couch2sql;

import com.google.gson.JsonObject;
import java.lang.Thread;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.lightcouch.ChangesResult.Row;
import org.lightcouch.CouchDbClient;
import org.lightcouch.Response;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.Wait;

import static org.junit.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for CouchReader.
 */
public class CouchReaderTest {

    @ClassRule
    public static GenericContainer couchContainer =
        new CustomCouchDBContainer();

    private static LinkedList<JsonObject> testData;

    @BeforeClass
    public static void beforeClass() {
        /* Come up with some test data & stuff it in the DB. */
        final int INSERT_SIZE = 50;
        final int DELETE_SIZE = 5;
        testData = new LinkedList<JsonObject>();
        for (int i = 0; i < INSERT_SIZE; i ++) {
            JsonObject o = new JsonObject();
            o.addProperty("_id", String.valueOf(i));
            o.addProperty("ordinal", i);
            testData.add(o);
        }

        CouchDbClient client = new CouchDbClient();
        for (JsonObject o : testData) {
            Response r = client.save(o);
            o.addProperty("_rev", r.getRev());
        }
        for (JsonObject o : testData.subList(0, DELETE_SIZE)) {
            Response r = client.remove(o);
            o.addProperty("_deleted", true);
            o.addProperty("_rev", r.getRev());
            o.remove("ordinal");
        }
    }

    @Test
    public void testConstructor()
    {
        PipedMock<Row> p = new PipedMock<Row>();
        CouchReader cr = new CouchReader(p);
        assertTrue(p.getSent().isEmpty());
    }

    @Test
    public void testSetSeq()
    {
        Piped<Row> p = new PipedMock<Row>();
        CouchReader cr = new CouchReader(p);
        cr.setSeq("");
    }

    @Test(timeout=20000)
    public void checkData() throws InterruptedException {
        PipedMock<Row> p = new PipedMock<Row>();
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
        t.join();

        JsonObject[] sent = (JsonObject[])
            p.getSent()
            .stream()
            .map(elem -> ((Row)elem).getDoc())
            .toArray(JsonObject[]::new);
        assertThat(sent)
            .containsExactlyInAnyOrderElementsOf(testData);
    }
}
