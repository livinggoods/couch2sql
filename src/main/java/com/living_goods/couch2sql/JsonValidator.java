package com.living_goods.couch2sql;

import java.io.InputStream;
import java.io.IOException;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.everit.json.schema.loader.SchemaLoader;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;

import org.json.JSONObject;
import org.json.JSONTokener;

import org.lightcouch.ChangesResult.Row;

/* Class which deserializes a JsonObject and passes it through,
 * subject to schema validation. Raises an exception if object does
 * not validate. */

public class JsonValidator implements Piped<Row> {
    private Piped<Row> target;
    private Schema schema;
    private static final Logger logger = LogManager.getLogger();

    private static final String SCHEMA_FILE = "/validate.json";

    JsonValidator(final Piped<Row> target) {
        this.target = target;

        try (InputStream inputStream =
             getClass().getResourceAsStream(SCHEMA_FILE)) {
            if (inputStream == null) {
                String msg = "Could not find JSON Schema in the classpath: "
                    + SCHEMA_FILE;
                logger.fatal(msg);
                throw new IllegalStateException(msg);
            }
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
    public void send(final Row input) {
        /* Skip if deleted. */
        if (!input.isDeleted()) {
            /* Convert JsonObject to JSONObject */
            final JSONObject jSONObject =
                new JSONObject(input.getDoc().toString());
            try {
                schema.validate(jSONObject);
            } catch (ValidationException e) {
                final JSONObject failures = e.toJSON();
                logger.fatal("JSON Validation failed for document "
                             + input.getDoc().get("_id").getAsString());
                logger.fatal(failures.toString(4), e);
                throw e;
            }
        }

        target.send(input);
    }

    /* We have no state here so this is a no-op. */
    @Override
    public void close() { }
}
