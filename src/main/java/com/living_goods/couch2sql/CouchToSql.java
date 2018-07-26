package com.living_goods.couch2sql;

public class CouchToSql
{
    public static void main( String[] args )
    {
        final SqlWriter writer = new SqlWriter();
        final JsonTransformer transformer = new JsonTransformer(writer);
        final JsonValidator validator = new JsonValidator(transformer);
        final CouchReader reader = new CouchReader(validator);
        reader.run();
    }
}
