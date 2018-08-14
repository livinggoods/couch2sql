package com.living_goods.couch2sql;

import com.fasterxml.jackson.databind.JsonNode;

import org.lightcouch.ChangesResult.Row;

/* This class just wraps a ChangesResult.Row (from CouchDB) with the
 * transformed JSON (from JSLT), so that we can hang on to the
 * sequence number for the change, when we go to update SQL Server. */
public class TransformedChange {
    private Row row;
    private JsonNode result;

    public TransformedChange(final Row row, final JsonNode result) {
        this.row = row;
        this.result = result;
    }

    public int hashCode() {
        final int hashRow = row != null ? row.hashCode() : 0;
        final int hashResult = result != null ? result.hashCode() : 0;

        return (hashRow + hashResult) * hashResult + hashRow;
    }

    public boolean equals(final Object other) {
        if (other instanceof TransformedChange) {
            final TransformedChange otherTC = (TransformedChange) other;
            return ((this.row == otherTC.row
                     || (this.row != null && otherTC.row != null
                         && this.row.equals(otherTC.row)))
                    &&
                    (this.result == otherTC.result
                     || (this.result != null && otherTC.result != null
                         && this.result.equals(otherTC.result))));
        }

        return false;
    }

    public String toString() {
        return "TransformedChange(" + row + ", " + result + ")";
    }

    public Row getRow() {
        return row;
    }

    public JsonNode getResult() {
        return result;
    }
}
