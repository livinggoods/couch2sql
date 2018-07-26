package com.living_goods.couch2sql;

import com.google.gson.JsonObject;
import org.lightcouch.CouchDbClient;
import org.lightcouch.Changes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/* Class which reads data from CouchDB and feeds it into a Piped. */
public class CouchReader {
    private Piped<JsonObject> target;
    private CouchDbClient client;
    private String seq;
    private Changes couchChanges;
    private static final Logger logger = LogManager.getLogger();

    CouchReader(Piped<JsonObject> target) {
        this.target = target;
        /* Connection parameters come from couchdb.properties file */
        this.client = new CouchDbClient();
    }

    /* Sets the initial sequence number used for the changes
     * feed. Must be called before run(). */
    public void setSeq(String seq) {
        this.seq = seq;
    }
    
    /* Fetches all records and feeds them into the pipe. Runs until
     * stop() is called. */
    public void run() {
        this.couchChanges = this.client.changes()
            .includeDocs(true)
            .since(this.seq)
            .heartBeat(300)
            .continuousChanges();
        while (this.couchChanges.hasNext()) {
            JsonObject doc = this.couchChanges.next().getDoc();
            target.send(doc);
        }
        this.couchChanges = null;
    }

    /* Stops a running iteration. Causes run() to exit, raises
     * NullPointerException if run() is not currently running. */
    public void stop() {
        this.couchChanges.stop();
    }
}
