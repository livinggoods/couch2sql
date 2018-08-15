package com.living_goods.couch2sql;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.schibsted.spt.data.jslt.Expression;
import com.schibsted.spt.data.jslt.Parser;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.lightcouch.ChangesResult.Row;

/* Class which filters input JSON documents according to a JSLT
 * expression. If the expression returns true, the document will be
 * passed through; otherwise it will be suppressed. */

public class JsonFilter implements Piped<Row> {
    private Piped<Row> target;
    private Expression filter;
    private ObjectMapper mapper;
    private static final Logger logger = LogManager.getLogger();

    private static final String EXPRESSION_FILE = "filter.jslt";

    JsonFilter(final Piped<Row> target) {
        this.target = target;

        filter = Parser.compileResource(EXPRESSION_FILE);

        mapper = new ObjectMapper();
    }

    @Override
    public void send(final Row input) {
        /* Just pass through if this is a deletion. Deletions don't
         * have any other data so there is nothing to filter. */
        if (input.isDeleted()) {
            target.send(input);
        } else {
            /* This is not the most efficient approach. We parse to
             * jackson here and then again in JsonTransformer. */

            /* Wierdly converting a String to a JsonNode can throw an
             * IOException. */
            final JsonNode inputNode;
            try {
                inputNode = mapper.readTree(input.getDoc().toString());
            } catch (IOException e) {
                logger.fatal("Could not convert JSON document to Jackson", e);
                throw new IllegalArgumentException(e);
            }
            final JsonNode outputNode = filter.apply(inputNode);
            if (outputNode.isBoolean()) {
                if (outputNode.booleanValue()) {
                    target.send(input);
                }
            } else {
                throw new IllegalArgumentException(
                    "Filter transform returned non-boolean");
            }
        }
    }

    /* We have no state here so this is a no-op. */
    @Override
    public void close() { }
}
