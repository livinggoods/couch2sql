package com.living_goods.couch2sql;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.Wait;

/* The main purpose of this class is the centralize the configuration
 * of the CouchDB Docker instance so that it can be used in both
 * CouchReaderTest.java and IntegrationTest.java. */
class CustomCouchDBContainer extends FixedHostPortGenericContainer {
    private static final String DOCKER_CONTAINER_NAME = "couchdb:1.6.1";

    public CustomCouchDBContainer() {
        super(DOCKER_CONTAINER_NAME);
        withFixedExposedPort(5985, 5984);
        waitingFor(Wait.forHttp("/")
                   .forStatusCode(200));
    }
}

