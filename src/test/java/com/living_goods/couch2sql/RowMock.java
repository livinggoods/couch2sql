package com.living_goods.couch2sql;

import org.lightcouch.ChangesResult.Row;
import com.google.gson.JsonObject;

/* Mock version of org.lightcouch.ChangesResult.Row, which allows
 * constructing with values. */

public class RowMock extends Row {
    String seq;
    JsonObject doc;
    
    RowMock(String sequence, JsonObject document) {
        this.seq = sequence;
        this.doc = document;
    }

    RowMock() {
        this.seq = "";
        this.doc = new JsonObject();
    }
    
    @Override
    public String getSeq() {
        return seq;
    }

    @Override
    public JsonObject getDoc() {
        return doc;
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
     * isDeleted, etc. */
}

 
