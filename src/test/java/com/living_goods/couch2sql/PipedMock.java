package com.living_goods.couch2sql;

import java.util.ArrayList;

/* Mocked out class implementing Piped and saving its results in an
 * ArrayList. */
public class PipedMock<I> implements Piped<I> {
    private ArrayList<I> sent;

    PipedMock() {
        sent = new ArrayList<I>();
    }

    @Override
    public void send(I input) {
        sent.add(input);
    }

    @Override
    public void close() {
        sent = null;
    }

    public ArrayList<I> getSent() {
        return sent;
    }
}
