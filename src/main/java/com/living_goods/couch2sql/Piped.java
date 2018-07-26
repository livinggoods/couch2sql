package com.living_goods.couch2sql;

/* Inspired / stolen from
 * http://www.pixeldonor.com/2013/nov/22/processing-pipelines-java/ */

/* Interface for classes which can receive data from a pipe. */
public interface Piped<I> extends java.io.Closeable  {
    /* This method will be called by the upstream pipe when it has
     * more data to send. */
    void send(I input);
}
