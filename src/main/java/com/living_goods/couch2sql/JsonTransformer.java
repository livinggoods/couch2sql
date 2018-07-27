package com.living_goods.couch2sql;

import java.io.IOException;
import com.google.gson.JsonObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.schibsted.spt.data.jslt.Parser;
import com.schibsted.spt.data.jslt.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lightcouch.ChangesResult.Row;

/* Class which transforms the input JSON document using a JSLT
 * expression, and passes the result into the next module. */

public class JsonTransformer implements Piped<Row> {
    private Piped<TransformedChange> target;
    private Expression transform;
    private ObjectMapper mapper;
    private static final Logger logger = LogManager.getLogger();

    private static final String EXPRESSION_FILE = "transform.jslt";

    JsonTransformer(Piped<TransformedChange> target) {
        this.target = target;

        transform = Parser.compileResource(EXPRESSION_FILE);

        mapper = new ObjectMapper();
    }

    
    @Override
    public void send(Row input) {
        /* Wierdly converting a String to a JsonNode can throw an
         * IOException. */
        JsonNode inputNode;
        try {
            inputNode = mapper.readTree(input.getDoc().toString());
        } catch (IOException e) {
            logger.fatal("Could not convert JSON document to Jackson", e);
            throw new IllegalArgumentException(e);
        }
        JsonNode outputNode = transform.apply(inputNode);
        target.send(new TransformedChange(input, outputNode));
    }

    /* We have no state here so this is a no-op. */
    @Override
    public void close() {}
}
