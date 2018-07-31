package com.living_goods.couch2sql;

import org.junit.Test;
import org.lightcouch.ChangesResult.Row;
import org.everit.json.schema.ValidationException;
import com.google.gson.JsonObject;
import com.fasterxml.jackson.databind.JsonNode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertSame;

import static org.assertj.core.api.Assertions.assertThat;

/* Unit test for JsonTransformer */
public class JsonTransformerTest {
    /* This test verifies that we can load the JSLT expression. */
    @Test
    public void testConstructor() {
        PipedMock<TransformedChange> p = new PipedMock<TransformedChange>();
        JsonTransformer jt = new JsonTransformer(p);
        assertTrue(p.getSent().isEmpty());
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

    /* Test basic JSON structure of output. */
    @Test
    public void testStructure() {
        PipedMock<TransformedChange> p = new PipedMock<TransformedChange>();
        JsonTransformer jt = new JsonTransformer(p);
        JsonObject testObj = makePerson();
        testObj.addProperty("reported_date", "1514764800");
        JsonObject fp_screening = new JsonObject();
        fp_screening.addProperty("risk_factors", "foo bar");
        testObj.add("fp_screening", fp_screening);
        Row r = new RowMock("", testObj);
        jt.send(r);
        assertSame(p.getSent().get(0).getRow(), r);
        JsonNode result = p.getSent().get(0).getResult();
        assertTrue(result.isObject());
        assertThat(result.fieldNames())
            .containsExactlyInAnyOrder("table", "row", "dimensions");
        assertTrue(result.get("table").isTextual());
        assertTrue(result.get("row").isObject());
        assertTrue(result.get("dimensions").isArray());
        assertTrue(result.get("dimensions").get(0).isObject());
        assertThat(result.get("dimensions").get(0).fieldNames())
            .containsExactlyInAnyOrder("table", "key_column", "key_value",
                                       "rows");
        assertTrue(result.get("dimensions").get(0).get("table").isTextual());
        assertTrue(result.get("dimensions").get(0)
                   .get("key_column").isTextual());
        assertTrue(result.get("dimensions").get(0)
                   .get("key_value").isTextual());
        assertEquals(result.get("dimensions").get(0)
                     .get("key_value").textValue(),
                     "foo");
        assertTrue(result.get("dimensions").get(0).get("rows")
                   .isArray());
    }
    
    /* Test epoch datetime transform. */
    @Test
    public void testDateTimeEpoch() {
        PipedMock<TransformedChange> p = new PipedMock<TransformedChange>();
        JsonTransformer jt = new JsonTransformer(p);
        JsonObject testObj = makePerson();
        testObj.addProperty("reported_date", "1514764800000");
        Row r = new RowMock("", testObj);
        jt.send(r);
        System.out.println(p.getSent());
        assertEquals("2018-01-01 00:00:00.000",
                     p.getSent().get(0).getResult().get("row")
                     .get("REPORTED_DATE").asText());
                     
    }
    
    /* Test yyyymmdd or yyyymmmdd transform. */

    /* Test ddmmyyyy or yyyymmdd transform. */

    /* Test equal values validation (pass). */

    /* Test equal values validation (fail). */

    /* Test suppression of empty strings. */

    /* Test boolean transforms */

}
