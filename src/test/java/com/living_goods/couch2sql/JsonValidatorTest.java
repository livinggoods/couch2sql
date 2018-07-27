package com.living_goods.couch2sql;

import org.junit.Test;
import org.lightcouch.ChangesResult.Row;
import org.everit.json.schema.ValidationException;
import com.google.gson.JsonObject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/* Unit test for JsonValidator */
public class JsonValidatorTest {
    /* This test verifies that we can load the schema document. */
    @Test
    public void testConstructor() {
        PipedMock<Row> p = new PipedMock<Row>();
        JsonValidator cr = new JsonValidator(p);
        assertTrue(p.getSent().isEmpty());
    }

    /* Test a passing document. */
    @Test
    public void testTransformPass() {
        PipedMock<Row> p = new PipedMock<Row>();
        JsonValidator cr = new JsonValidator(p);
        JsonObject testObj = new JsonObject();
        testObj.addProperty("_id", "foo");
        testObj.addProperty("_rev", "1-23202479633c2b380f79507a776743d5");
        testObj.addProperty("name", "John Doe");
        testObj.addProperty("type", "person");
        Row r = new RowMock("", testObj);
        cr.send(r);
        assertEquals(p.getSent().size(), 1);
        assertEquals(p.getSent().get(0), r);
    }

    /* Test a failing document. */
    @Test(expected = ValidationException.class)
    public void testTransformFail() {
        Piped p = new PipedMock<Row>();
        JsonValidator cr = new JsonValidator(p);
        JsonObject testObj = new JsonObject();
        testObj.addProperty("_id", "foo");
        testObj.addProperty("_rev", "1-23202479633c2b380f79507a776743d5");
        testObj.addProperty("type", "person");
        testObj.addProperty("foo", "bar");
        /* Should raise */
        cr.send(new RowMock("", testObj));
    }
}
