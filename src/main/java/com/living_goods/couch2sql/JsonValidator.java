package com.living_goods.couch2sql;

import java.io.InputStream;
import java.io.IOException;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
/* Note different capitalization. They are incompatible libraries. */
import com.google.gson.JsonObject;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/* Class which deserializes a JsonObject and passes it through,
 * subject to schema validation. Raises an exception if object does
 * not validate. */

public class JsonValidator implements Piped<JsonObject> {
    private Piped<String> target;
    private Schema schema;
    private static final Logger logger = LogManager.getLogger();

    private static final String SCHEMA_FILE = "validate.json";
    
    JsonValidator(Piped<String> target) {
        this.target = target;
        
        try (InputStream inputStream =
             getClass().getResourceAsStream(SCHEMA_FILE)) {
            JSONObject rawSchema =
                new JSONObject(new JSONTokener(inputStream));
            schema = SchemaLoader.load(rawSchema);
        } catch (IOException e) {
            String msg = "Could not read JSON Schema from the classpath: "
                + SCHEMA_FILE;
            logger.fatal(msg, e);
            throw new IllegalStateException(msg, e);
        }
    }

    @Override
    public void send(JsonObject input) {
        /* Convert JsonObject to JSONObject */
        String jsonString = input.toString();
        JSONObject jSONObject = new JSONObject(jsonString);
        /* Raises an exception if validation fails. */
        schema.validate(jSONObject);
        
        target.send(jsonString);
    }

    /* We have no state here so this is a no-op. */
    @Override
    public void close() {}
}
