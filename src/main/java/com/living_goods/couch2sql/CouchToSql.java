package com.living_goods.couch2sql;

import java.lang.Runnable;
import org.boris.winrun4j.Service;
import org.boris.winrun4j.EventLog;
import org.boris.winrun4j.ServiceException;

public class CouchToSql implements Service, Runnable {
    public static void main(String[] args)
    {
        CouchToSql instance = new CouchToSql();
        instance.run();
    }

    @Override
    public int serviceMain(String[] args) {
        run();
        return 0; /* Ignored? */
    }

    @Override
    public int serviceRequest(int control) {
        switch (control) {
        case SERVICE_CONTROL_STOP:
        case SERVICE_CONTROL_SHUTDOWN:
            stop();
            break;
        }
        return 0; /* Ignored? */
    }
    
    private SqlWriter writer;
    private CouchReader reader;
    
    CouchToSql() {
        writer = new SqlWriter();
        final JsonTransformer transformer = new JsonTransformer(writer);
        final JsonValidator validator = new JsonValidator(transformer);
        reader = new CouchReader(validator);
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
