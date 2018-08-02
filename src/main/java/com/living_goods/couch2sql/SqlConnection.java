package com.living_goods.couch2sql;

import java.sql.SQLException;
import java.sql.Connection;
import java.util.Properties;
import java.io.InputStream;
import java.io.IOException;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/* Class to set up the SQL Server database connection using classpath
 * properties. */
public class SqlConnection {
    private static final Logger logger = LogManager.getLogger();
    public static final String DATA_SOURCE_CONTEXT = "sqldb";

    /* Finds connection parameters from the classpath, creates a
     * DataSource, and returns the resulting Connection. */
    public static Connection getSqlConnection() {
        /* This is a bit of a hack. The right way is to use JNDI to
           get a DataSource, but I couldn't find a JNDI implementation
           that I liked (wanted to load the data source properties
           from the classpath). Instead we find the properties file
           ourselves, and then use the Apache DBCP2
           BasicDataSourceFactory to convert the properties file into
           a DataSource.

           I have opened an enhancement request for Simple-JNDI to
           support loading properties from the classpath, after which
           the lookup could be done in a more portable way with
           InitialContext.lookup().
           https://github.com/h-thurow/Simple-JNDI/issues/9
        */
        try {
            final Properties properties = new Properties();
            try (InputStream configStream =
                     SqlConnection.class
                     .getResourceAsStream("/" + DATA_SOURCE_CONTEXT +
                                          ".properties")) {
                properties.load(configStream);
            } catch (IOException e) {
                String msg = "Could not read SQL Server configuration from the classpath: "
                    + DATA_SOURCE_CONTEXT;
                logger.fatal(msg, e);
                throw new IllegalStateException(msg, e);
            }
            DataSource dataSource;
            try {
                final BasicDataSourceFactory loader =
                    new BasicDataSourceFactory();
                dataSource = loader.createDataSource(properties);
            } catch (Exception e) {
                /* Bad API just throws Exception. */
                String msg = "Could not create SQL Server DataSource.";
                logger.fatal(msg, e);
                throw new IllegalStateException(msg, e);
            }
            
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);

            /* We really should only have one writer at a time. */
            final int tiso = Connection.TRANSACTION_SERIALIZABLE;
            connection.setTransactionIsolation(tiso);

            return connection;
        } catch (SQLException e) {
            logger.fatal("Error setting up SQL Server connection", e);
            throw new IllegalStateException(e);
        }
    }

    private SqlConnection() {};
}
