package com.living_goods.couch2sql;

import java.io.IOException;
import com.google.gson.JsonObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.schibsted.spt.data.jslt.Parser;
import com.schibsted.spt.data.jslt.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/* Class which transforms the input JSON document using a JSLT
 * expression, and passes the result into the next module. */

public class JsonTransformer implements Piped<String> {
    private Piped<JsonNode> target;
    private Expression transform;
    private ObjectMapper mapper;
    private static final Logger logger = LogManager.getLogger();

    private static final String EXPRESSION_FILE = "transform.jslt";

    JsonTransformer(Piped<JsonNode> target) {
        this.target = target;

        transform = Parser.compileResource(EXPRESSION_FILE);

        mapper = new ObjectMapper();
    }

    
    @Override
    public void send(String input) {
        /* Wierdly converting a String to a JsonNode can throw an
         * IOException. */
        JsonNode inputNode;
        try {
            inputNode = mapper.readTree(input);
        } catch (IOException e) {
            logger.fatal("Could not convert JSON document to Jackson", e);
            throw new IllegalArgumentException(e);
        }
        JsonNode outputNode = transform.apply(inputNode);
        target.send(outputNode);
    }

    /* We have no state here so this is a no-op. */
    @Override
    public void close() {}
}
