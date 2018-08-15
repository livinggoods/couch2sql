package com.living_goods.couch2sql;

import org.boris.winrun4j.Service;

public class CouchToSql implements Service, Runnable {
    public static void main(final String[] args) {
        final CouchToSql instance = new CouchToSql();
        instance.run();
    }

    @Override
    public int serviceMain(final String[] args) {
        run();
        return 0; /* Ignored? */
    }

    @Override
    public int serviceRequest(final int control) {
        switch (control) {
        case SERVICE_CONTROL_STOP:
        case SERVICE_CONTROL_SHUTDOWN:
            stop();
            break;
        default:
        }
        return 0; /* Ignored? */
    }

    private SqlWriter writer;
    private CouchReader reader;

    CouchToSql() {
        writer = new SqlWriter();
        final JsonTransformer transformer = new JsonTransformer(writer);
        final JsonValidator validator = new JsonValidator(transformer);
        final JsonFilter filter = new JsonFilter(validator);
        reader = new CouchReader(filter);
        reader.setSeq(writer.getSeq());
    }

    @Override
    public void run() {
        reader.run();
    }

    public void stop() {
        reader.stop();
    }
}
