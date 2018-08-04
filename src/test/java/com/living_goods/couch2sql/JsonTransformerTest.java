package com.living_goods.couch2sql;

import org.junit.Test;
import org.lightcouch.ChangesResult.Row;
import com.google.gson.JsonObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.schibsted.spt.data.jslt.JsltException;

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
        Row r = RowMock.make("", testObj);
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
    public void testDateTime1() {
        PipedMock<TransformedChange> p = new PipedMock<TransformedChange>();
        JsonTransformer jt = new JsonTransformer(p);
        JsonObject testObj = makePerson();
        testObj.addProperty("reported_date", "1514764800000");
        Row r = RowMock.make("", testObj);
        jt.send(r);
        assertEquals("2018-01-01 00:00:00.000",
                     p.getSent().get(0).getResult().get("row")
                     .get("REPORTED_DATE").asText());
                     
    }
    
    /* Test yyyymmdd or yyyymmmdd transform. */
    @Test
    public void testDateTime2() {
        PipedMock<TransformedChange> p = new PipedMock<TransformedChange>();
        JsonTransformer jt = new JsonTransformer(p);

        JsonObject testObj = makePerson();
        testObj.addProperty("date_of_birth", "2018-01-02");
        jt.send(RowMock.make("", testObj));

        testObj = makePerson();
        testObj.addProperty("date_of_birth", "20180102");
        jt.send(RowMock.make("", testObj));

        testObj = makePerson();
        testObj.addProperty("date_of_birth", "2018-Jan-02");
        jt.send(RowMock.make("", testObj));

        for (TransformedChange tc : p.getSent()) {
            assertEquals("2018-01-02",
                         tc.getResult().get("row").get("DATE_OF_BIRTH")
                         .asText());
        }
    }

    /* Test ddmmyyyy or yyyymmdd transform. */
    @Test
    public void testDateTime3() {
        PipedMock<TransformedChange> p = new PipedMock<TransformedChange>();
        JsonTransformer jt = new JsonTransformer(p);

        JsonObject testObj = makePerson();
        testObj.addProperty("date_of_graduation", "2018-01-20");
        jt.send(RowMock.make("", testObj));

        testObj = makePerson();
        testObj.addProperty("date_of_graduation", "20180120");
        jt.send(RowMock.make("", testObj));

        testObj = makePerson();
        testObj.addProperty("date_of_graduation", "01/20/2018");
        jt.send(RowMock.make("", testObj));

        for (TransformedChange tc : p.getSent()) {
            assertEquals("2018-01-20",
                         tc.getResult().get("row").get("DATE_OF_GRADUATION")
                         .asText());
        }
    }

    /* Test equal values validation (pass). */
    @Test
    public void testIdenticalPass() {
        PipedMock<TransformedChange> p = new PipedMock<TransformedChange>();
        JsonTransformer jt = new JsonTransformer(p);
        JsonObject testObj = makePerson();
        testObj.addProperty("relationship_to_primary_caregiver", "self");
        testObj.addProperty("relationship_primary_caregiver", "self");

        Row r = RowMock.make("", testObj);
        jt.send(r);
        assertEquals("self",
                     p.getSent().get(0).getResult().get("row")
                     .get("RELATIONSHIP_TO_PRIMARY_CAREGIVER").asText());
    }

    /* Test equal values validation (fail). */
    @Test(expected = JsltException.class)
    public void testIdenticalFail() {
        PipedMock<TransformedChange> p = new PipedMock<TransformedChange>();
        JsonTransformer jt = new JsonTransformer(p);
        JsonObject testObj = makePerson();
        testObj.addProperty("relationship_to_primary_caregiver", "daughter");
        testObj.addProperty("relationship_primary_caregiver", "son");

        Row r = RowMock.make("", testObj);
        jt.send(r);
    }

    /* Test suppression of empty strings. */
    @Test
    public void testAbsentValues() {
        PipedMock<TransformedChange> p = new PipedMock<TransformedChange>();
        JsonTransformer jt = new JsonTransformer(p);
        JsonObject testObj = makePerson();
        testObj.addProperty("first_name", "");
        testObj.addProperty("phone", "");
        testObj.addProperty("reported_date", "");
        testObj.addProperty("date_of_graduation", "");
        testObj.addProperty("date_of_birth", "");
        testObj.addProperty("pregnant_at_registration", "");

        /* We should get the same result as we would have gotten
         * without adding all these empty fields. */
        jt.send(RowMock.make("", testObj));
        jt.send(RowMock.make("", makePerson()));
        assertEquals(p.getSent().get(1).getResult(),
                     p.getSent().get(0).getResult());
    }

    /* Test boolean transforms */
    @Test
    public void testBoolean() {
        PipedMock<TransformedChange> p = new PipedMock<TransformedChange>();
        JsonTransformer jt = new JsonTransformer(p);
        JsonObject testObj = makePerson();
        testObj.addProperty("pregnant_at_registration", "true");
        testObj.addProperty("fp_eligible", "yes");
        testObj.addProperty("delivered_in_facility", true);
        testObj.addProperty("exclusively_breastfed", "false");
        testObj.addProperty("slept_under_treated_net", "no");
        testObj.addProperty("cant_pay_now", false);

        jt.send(RowMock.make("", testObj));
        JsonNode row = p.getSent().get(0).getResult().get("row");
        assertEquals(BooleanNode.TRUE, row.get("PREGNANT_AT_REGISTRATION"));
        assertEquals(BooleanNode.TRUE, row.get("FP_ELIGIBLE"));
        assertEquals(BooleanNode.TRUE, row.get("DELIVERED_IN_FACILITY"));
        assertEquals(BooleanNode.FALSE, row.get("EXCLUSIVELY_BREASTFED"));
        assertEquals(BooleanNode.FALSE, row.get("SLEPT_UNDER_TREATED_NET"));
        assertEquals(BooleanNode.FALSE, row.get("CANT_PAY_NOW"));
    }

    /* Test sending a deleted document. */
    @Test
    public void testDeleted() {
        PipedMock<TransformedChange> p = new PipedMock<TransformedChange>();
        JsonTransformer jt = new JsonTransformer(p);
        Row sentRow = RowMock.makeDeletion("", "foo");
        jt.send(sentRow);
        Row row = p.getSent().get(0).getRow();
        assertSame(row, sentRow);
    }
}
