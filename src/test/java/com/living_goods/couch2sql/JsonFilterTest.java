package com.living_goods.couch2sql;

import org.junit.Test;
import org.lightcouch.ChangesResult.Row;
import com.google.gson.JsonObject;

import static org.junit.Assert.assertSame;

import static org.assertj.core.api.Assertions.assertThat;

/* Unit test for JsonFilter */
public class JsonFilterTest {
    /* This test verifies that we can load the JSLT expression. */
    @Test
    public void testConstructor() {
        PipedMock<Row> p = new PipedMock<Row>();
        JsonFilter jt = new JsonFilter(p);
        assertThat(p.getSent()).isEmpty();
    }

    /* Constructs a Person object and populates basic fields. */
    static private JsonObject makePerson() {
        JsonObject testObj = new JsonObject();
        testObj.addProperty("_id", "foo");
        testObj.addProperty("_rev", "1-23202479633c2b380f79507a776743d5");
        testObj.addProperty("name", "John Doe");
        testObj.addProperty("type", "person");
        return testObj;
    }

    /* Test sending a passing document. */
    @Test
    public void testPass() {
        PipedMock<Row> p = new PipedMock<Row>();
        JsonFilter jf = new JsonFilter(p);

        JsonObject testObj = new JsonObject();
        testObj.addProperty("type", "district_hospital");
        Row r = RowMock.make("", testObj);
        jf.send(r);
        assertThat(p.getSent()).hasSize(1);
        assertSame(p.getSent().get(0), r);
    }
    
    /* Test sending a filtered document. */
    @Test
    public void testFail() {
        PipedMock<Row> p = new PipedMock<Row>();
        JsonFilter jf = new JsonFilter(p);

        JsonObject testObj = new JsonObject();
        testObj.addProperty("foo", "bar");
        Row r = RowMock.make("", testObj);
        jf.send(r);
        assertThat(p.getSent()).isEmpty();
    }
    
    /* Test sending a deleted document. */
    @Test
    public void testDeleted() {
        PipedMock<Row> p = new PipedMock<Row>();
        JsonFilter jt = new JsonFilter(p);
        Row sentRow = RowMock.makeDeletion("", "foo");
        jt.send(sentRow);
        Row row = p.getSent().get(0);
        assertThat(p.getSent()).hasSize(1);
        assertSame(row, sentRow);
    }
}
