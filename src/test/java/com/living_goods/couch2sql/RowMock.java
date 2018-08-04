package com.living_goods.couch2sql;

import org.lightcouch.ChangesResult.Row;
import com.google.gson.JsonObject;

/* Mock version of org.lightcouch.ChangesResult.Row, which allows
 * constructing with values. */

public class RowMock extends Row {
    String seq;
    JsonObject doc;
    String id;

    private RowMock(String sequence, JsonObject document, String id) {
        this.seq = sequence;
        this.doc = document;
        this.id = id;
    }

    public static RowMock make(String sequence, JsonObject document) {
        if (document == null) {
            document = new JsonObject();
        }
        return new RowMock(sequence, document, null);
    }

    public static RowMock make(String sequence) {
        return new RowMock(sequence, new JsonObject(), null);
    }

    public static RowMock makeDeletion(String sequence, String id) {
        return new RowMock(sequence, null, id);
    }
    
    @Override
    public String getSeq() {
        return seq;
    }

    @Override
    public JsonObject getDoc() {
        return doc;
    }

    @Override
    public String getId() {
        /* Only supported for deletions, currently. */
        assert doc == null;
        return id;
    }

    @Override
    public boolean isDeleted() {
        return (doc == null);
    }
    
    public boolean equals(Object other) {
        if (other instanceof Row) {
            Row otherRow = (Row) other;
            return ((this.seq == otherRow.getSeq() ||
                     (this.seq != null && otherRow.getSeq() != null &&
                      this.seq.equals(otherRow.getSeq()))) &&
                    (this.doc == otherRow.getDoc() ||
                     (this.doc != null && otherRow.getDoc() != null &&
                      this.doc.equals(otherRow.getDoc()))));
        }

        return false;
    }

    /* Note other methods not provided, e.g., getId, getChanges,
     * etc. */
}

 
