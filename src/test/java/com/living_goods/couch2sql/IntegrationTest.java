package com.living_goods.couch2sql;

import java.lang.Thread;
import org.junit.Test;
import org.junit.ClassRule;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.HostPortWaitStrategy;
import org.testcontainers.containers.wait.Wait;
import org.lightcouch.CouchDbClient;
import org.lightcouch.Response;
import com.google.gson.JsonObject;
import com.google.gson.JsonStreamParser;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.io.InputStream;
import java.io.Reader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

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
    public void testCouchToSql()
        throws InterruptedException, SQLException,
               UnsupportedEncodingException {
        /* Push stuff to CouchDB. */
        CouchDbClient client = new CouchDbClient();

        /* Changes feed fails if the database is empty, put something
         * there before we start the reader. */
        JsonObject o = new JsonObject();
        o.addProperty("_id", "IntegrationTestBegin");
        o.addProperty("type", "person");
        o.addProperty("name", "Sentinel for start of data processing.");
        client.save(o);
        
        /* Run CouchToSql in a thread. */
        CouchToSql instance = new CouchToSql();
        Thread t = new Thread(instance);
        t.start();
        
        for (JsonObject o2 : loadTestObjects()) {
            o2.remove("_rev");
            client.save(o2);
        }
        
        o = new JsonObject();
        o.addProperty("_id", "IntegrationTestEnd");
        o.addProperty("type", "person");
        o.addProperty("name", "Sentinel for end of data processing.");
        client.save(o);

        /* Run until we see the sentinel in SQL Server. */
        Connection connection = mssqlserver.getConnection();
        PreparedStatement statement = connection.prepareStatement
            ("SELECT COUNT(*) FROM COUCHDB_IDS " +
             "WHERE ID = 'IntegrationTestEnd';");

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

        ResultSet rs = connection.createStatement()
            .executeQuery("SELECT COUNT(*) FROM PERSON;");
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 4);
        assertFalse(rs.next());
        
        assertDB1(connection);
        assertDB2(connection);
    }

    /* Returns a container with the test JSON objects to load. */
    private Iterable<JsonObject> loadTestObjects()
        throws UnsupportedEncodingException {
        Reflections reflections = new Reflections("IntegrationTest",
                                                  new ResourcesScanner());
        /* Reflections gives us a HashSet, but we need the result to
         * be sorted in case there are dependencies in the objects. */
        Pattern pattern = Pattern.compile(".*\\.json");
        Set<String> objects = new TreeSet(reflections.getResources(pattern));

        assertFalse(objects.isEmpty());

        ArrayList<JsonObject> rval =
            new ArrayList<JsonObject>(objects.size());
        for (String resourceName : objects) {
            InputStream is =
                getClass().getResourceAsStream("/" + resourceName);
            Reader r = new InputStreamReader(is, "UTF-8");
            JsonStreamParser p = new JsonStreamParser(r);
            assertTrue(p.hasNext());
            rval.add((JsonObject)p.next());
            /* Enforce one document per file. Not strictly necessary. */
            assertFalse(p.hasNext());
        }
        return rval;
    }

    /* Check the object with ID 9c0a1135-abd0-4075-b3c8-0f48a02cd4b1 */
    private void assertDB1(Connection connection) throws SQLException {
        PreparedStatement s =
            connection.prepareStatement("SELECT * FROM PERSON WHERE ID = ?;");
        final String id = "9c0a1135-abd0-4075-b3c8-0f48a02cd4b1";
        s.setString(1, id);
        ResultSet rs = s.executeQuery();
        assertTrue(rs.next());

        /* n.b. We can't use assertTrue, assertFalse, getInt,
         * getDouble, or getBoolean here because they don't handle
         * null values correctly. */
        assertEquals(rs.getString("ID"), id);
        assertEquals(rs.getString("NAME"), "Redacted Name");
        assertNull(rs.getString("FIRST_NAME"));
        assertNull(rs.getString("LAST_NAME"));
        assertNull(rs.getString("COMMON_NAME"));
        assertNull(rs.getString("DISPLAY_NAME"));
        assertNull(rs.getString("NOTES"));        
        assertEquals(rs.getString("SEX"), "female");
        assertNull(rs.getString("PHONE"));        
        assertNull(rs.getString("ALTERNATE_PHONE"));        
        assertNull(rs.getString("PHONE_OWNER"));        
        assertEquals(rs.getString("PARENT"),
                     "4498c33b-35f1-4732-97f4-a90dbc4a0748");
        assertNull(rs.getString("ROLE"));        
        assertEquals(rs.getString("REPORTED_DATE"),
                     "2018-05-08 16:47:05.520");
        assertNull(rs.getString("EXTERNAL_ID"));
        assertNull(rs.getString("FACILITY_ID"));
        assertNull(rs.getString("PATIENT_ID"));
        assertNull(rs.getString("LINK_FACILITY"));
        assertNull(rs.getString("COHORT_NUMBER"));
        assertNull(rs.getString("DATE_OF_GRADUATION"));
        assertEquals(rs.getString("DATE_OF_BIRTH"), "2018-03-19");
        assertThat((Boolean)rs.getObject("PREGNANT_AT_REGISTRATION"))
            .isFalse();
        assertNull((Boolean)rs.getObject("FP_ELIGIBLE"));
        assertNull((Boolean)rs.getObject("DELIVERED_IN_FACILITY"));
        assertNull((Boolean)rs.getObject("EXCLUSIVELY_BREASTFED"));
        assertThat((Boolean)rs.getObject("SLEPT_UNDER_TREATED_NET")).isFalse();
        assertEquals(rs.getString("RELATIONSHIP_TO_PRIMARY_CAREGIVER"),
                     "child");
        assertNull((Boolean)rs.getObject("RELATIONSHIP_PC_OTHER"));
        assertThat((Boolean)rs.getObject("CANT_PAY_NOW")).isFalse();
        assertNull(rs.getString("FP_G_EDD"));
        assertNull(rs.getString("FP_G_LMP_METHOD"));
        assertNull((Integer)rs.getObject("FP_G_LMP_APPROX"));
        assertNull(rs.getString("FP_CURRENT_METHOD"));
        assertNull(rs.getString("FP_WHY_STOP"));
        assertNull((Double)rs.getObject("FP_DAYS_SINCE_LMP"));
        assertNull(rs.getString("FP_G_LMP_CALENDAR"));
        assertNull((Boolean)rs.getObject("FP_BREASTFEEDING"));
        assertNull(rs.getString("FP_NEW_FP_METHOD"));
        assertNull((Boolean)rs.getObject("FP_TAKE_ON_SWITCH"));
        assertNull((Boolean)rs.getObject("FP_USED_MODERN_FP"));
        assertNull((Boolean)rs.getObject("FP_USING_MODERN_FP"));
        assertNull((Boolean)rs.getObject("FP_IN_THE_HOUSEHOLD"));
        assertNull((Boolean)rs.getObject("FP_SPOUSE_INCLUDED_HES"));
        assertNull((Integer)rs.getObject("FP_BREASTFEEDING_DURATION"));
        assertNull(rs.getString("FP_MOST_RECENT_METHOD"));
        assertNull(rs.getString("FP_PREGNANCY_TEST"));
        assertNull((Boolean)rs.getObject("FP_CONFIRMED_PREGNANCY"));
        assertNull(rs.getString("FP_METHOD_ADMINISTERED"));
        assertNull(rs.getString("LMP_DATE"));
        assertThat((Boolean)rs.getObject("ON_PILL")).isFalse();
        assertThat((Boolean)rs.getObject("ON_INJECTION")).isFalse();
        assertThat((Boolean)rs.getObject("NEWLY_ON_LAM")).isFalse();
        assertThat((Boolean)rs.getObject("NEWLY_ON_PILL")).isFalse();
        assertThat((Boolean)rs.getObject("NEWLY_ON_INJECTION")).isFalse();
        assertThat((Boolean)rs.getObject("ELIGIBLE_WOMAN")).isTrue();
        assertThat((Boolean)rs.getObject("PARTNER_REFUSE")).isFalse();
        assertNull((Integer)rs.getObject("UNPROTECTED_SEX"));
        assertThat((Boolean)rs.getObject("HAS_RISK_FACTORS")).isFalse();
        assertThat((Boolean)rs.getObject("IS_REFERRAL_CASE")).isFalse();
        assertThat((Boolean)rs.getObject("FEAR_SIDE_EFFECTS")).isFalse();
        assertNull(rs.getString("REASON_NOT_SWITCH"));
        assertThat((Boolean)rs.getObject("NEED_MORE_INFORMATION")).isFalse();
        assertNull((Boolean)rs.getObject("MOTHER_ATTENDED_ANC"));
        assertNull(rs.getString("COMMUNITY_UNIT"));
        assertNull(rs.getString("MARITAL_STATUS"));
        assertNull((Boolean)rs.getObject("IS_IN_EMNCH_PILOT"));
        assertNull((Boolean)
                   rs.getObject("IS_IN_DISPERSIBLE_AMOXICILLIN_PILOT"));
        assertNull((Boolean)rs.getObject("IS_IN_ECD_PILOT"));
        assertNull((Boolean)rs.getObject("IS_IN_RBF_PILOT"));
        assertNull((Boolean)rs.getObject("IS_IN_CDBS_PILOT"));
        assertNull((Boolean)rs.getObject("IS_IN_FP_PILOT"));
        assertNull(rs.getString("MOTHER_HIV_STATUS"));
        assertNull((Integer)rs.getObject("TOTAL_SCORE"));
        assertNull(rs.getString("NATIONAL_ID_NUMBER"));

        assertFalse(rs.next());

        s = connection.prepareStatement
            ("SELECT COUNT(*) FROM PERSON_FP_RISK_FACTORS " +
             "WHERE PERSON_ID = ?;");
        s.setString(1, id);
        rs = s.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 0);
        assertFalse(rs.next());
    }

    /* Check the object with ID 14e78dab-fe90-4547-a7bf-26576615a9f0 */
    private void assertDB2(Connection connection) throws SQLException {
        PreparedStatement s =
            connection.prepareStatement("SELECT * FROM PERSON WHERE ID = ?;");
        final String id = "14e78dab-fe90-4547-a7bf-26576615a9f0";
        s.setString(1, id);
        ResultSet rs = s.executeQuery();
        assertTrue(rs.next());

        /* n.b. We can't use assertTrue, assertFalse, getInt,
         * getDouble, or getBoolean here because they don't handle
         * null values correctly. */
        assertEquals(rs.getString("ID"), id);
        assertEquals(rs.getString("NAME"), "Redacted Name");
        assertNull(rs.getString("FIRST_NAME"));
        assertNull(rs.getString("LAST_NAME"));
        assertNull(rs.getString("COMMON_NAME"));
        assertNull(rs.getString("DISPLAY_NAME"));
        assertNull(rs.getString("NOTES"));        
        assertEquals(rs.getString("SEX"), "female");
        assertNull(rs.getString("PHONE"));        
        assertNull(rs.getString("ALTERNATE_PHONE"));        
        assertNull(rs.getString("PHONE_OWNER"));        
        assertEquals(rs.getString("PARENT"),
                     "81fc1f12-fd9b-4be6-ba44-9e4b81c14f07");
        assertNull(rs.getString("ROLE"));        
        assertEquals(rs.getString("REPORTED_DATE"),
                     "2018-04-20 07:13:07.288");
        assertNull(rs.getString("EXTERNAL_ID"));
        assertNull(rs.getString("FACILITY_ID"));
        assertNull(rs.getString("PATIENT_ID"));
        assertNull(rs.getString("LINK_FACILITY"));
        assertNull(rs.getString("COHORT_NUMBER"));
        assertNull(rs.getString("DATE_OF_GRADUATION"));
        assertEquals(rs.getString("DATE_OF_BIRTH"), "1996-04-20");
        assertThat((Boolean)rs.getObject("PREGNANT_AT_REGISTRATION"))
            .isFalse();
        assertNull((Boolean)rs.getObject("FP_ELIGIBLE"));
        assertNull((Boolean)rs.getObject("DELIVERED_IN_FACILITY"));
        assertNull((Boolean)rs.getObject("EXCLUSIVELY_BREASTFED"));
        assertThat((Boolean)rs.getObject("SLEPT_UNDER_TREATED_NET")).isTrue();
        assertEquals(rs.getString("RELATIONSHIP_TO_PRIMARY_CAREGIVER"),
                     "child");
        assertNull((Boolean)rs.getObject("RELATIONSHIP_PC_OTHER"));
        assertThat((Boolean)rs.getObject("CANT_PAY_NOW")).isFalse();
        assertEquals(rs.getString("FP_G_EDD"), "2018-12-18");
        assertEquals(rs.getString("FP_G_LMP_METHOD"), "calendar");
        assertNull((Integer)rs.getObject("FP_G_LMP_APPROX"));
        assertNull(rs.getString("FP_CURRENT_METHOD"));
        assertEquals(rs.getString("FP_WHY_STOP"), "side_effects");
        assertEquals((Double)rs.getObject("FP_DAYS_SINCE_LMP"),
                     new Double(38.3));
        assertEquals(rs.getString("FP_G_LMP_CALENDAR"), "2018-03-13");
        assertThat((Boolean)rs.getObject("FP_BREASTFEEDING")).isFalse();
        assertEquals(rs.getString("FP_NEW_FP_METHOD"), "cycle_beads");
        assertThat((Boolean)rs.getObject("FP_TAKE_ON_SWITCH")).isTrue();
        assertThat((Boolean)rs.getObject("FP_USED_MODERN_FP")).isTrue();
        assertThat((Boolean)rs.getObject("FP_USING_MODERN_FP")).isFalse();
        assertThat((Boolean)rs.getObject("FP_IN_THE_HOUSEHOLD")).isTrue();
        assertNull((Boolean)rs.getObject("FP_SPOUSE_INCLUDED_HES"));
        assertNull((Integer)rs.getObject("FP_BREASTFEEDING_DURATION"));
        assertEquals(rs.getString("FP_MOST_RECENT_METHOD"), "cocs");
        assertEquals(rs.getString("FP_PREGNANCY_TEST"), "negative");
        assertThat((Boolean)rs.getObject("FP_CONFIRMED_PREGNANCY")).isFalse();
        assertEquals(rs.getString("FP_METHOD_ADMINISTERED"), "Cycle beads");
        assertEquals(rs.getString("LMP_DATE"), "2018-03-13");
        assertThat((Boolean)rs.getObject("ON_PILL")).isFalse();
        assertThat((Boolean)rs.getObject("ON_INJECTION")).isFalse();
        assertThat((Boolean)rs.getObject("NEWLY_ON_LAM")).isFalse();
        assertThat((Boolean)rs.getObject("NEWLY_ON_PILL")).isFalse();
        assertThat((Boolean)rs.getObject("NEWLY_ON_INJECTION")).isFalse();
        assertNull((Boolean)rs.getObject("ELIGIBLE_WOMAN"));
        assertThat((Boolean)rs.getObject("PARTNER_REFUSE")).isFalse();
        assertNull((Integer)rs.getObject("UNPROTECTED_SEX"));
        assertThat((Boolean)rs.getObject("HAS_RISK_FACTORS")).isFalse();
        assertThat((Boolean)rs.getObject("IS_REFERRAL_CASE")).isFalse();
        assertThat((Boolean)rs.getObject("FEAR_SIDE_EFFECTS")).isFalse();
        assertNull(rs.getString("REASON_NOT_SWITCH"));
        assertThat((Boolean)rs.getObject("NEED_MORE_INFORMATION")).isFalse();
        assertNull((Boolean)rs.getObject("MOTHER_ATTENDED_ANC"));
        assertNull(rs.getString("COMMUNITY_UNIT"));
        assertNull(rs.getString("MARITAL_STATUS"));
        assertNull((Boolean)rs.getObject("IS_IN_EMNCH_PILOT"));
        assertNull((Boolean)
                   rs.getObject("IS_IN_DISPERSIBLE_AMOXICILLIN_PILOT"));
        assertNull((Boolean)rs.getObject("IS_IN_ECD_PILOT"));
        assertNull((Boolean)rs.getObject("IS_IN_RBF_PILOT"));
        assertNull((Boolean)rs.getObject("IS_IN_CDBS_PILOT"));
        assertNull((Boolean)rs.getObject("IS_IN_FP_PILOT"));
        assertNull(rs.getString("MOTHER_HIV_STATUS"));
        assertNull((Integer)rs.getObject("TOTAL_SCORE"));
        assertNull(rs.getString("NATIONAL_ID_NUMBER"));

        assertFalse(rs.next());

        s = connection.prepareStatement
            ("SELECT RISK_FACTOR FROM PERSON_FP_RISK_FACTORS " +
             "WHERE PERSON_ID = ?;");
        s.setString(1, id);
        rs = s.executeQuery();
        assertTrue(rs.next());
        assertEquals(rs.getString("RISK_FACTOR"), "no_risk");
        assertFalse(rs.next());
    }

}
