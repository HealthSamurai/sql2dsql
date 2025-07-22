package com.example.pgquery;


public class PgQuery {
    
    private static PgQueryLibInterface lib = PgQueryLibInterface.INSTANCE;

    public static String parse(String query) throws Exception {
        var res = lib.pg_query_parse(query);

        if (res.error == null) {
            return res.parse_tree;
        } else {
            throw new Exception(res.error.message);
        }
    }

}
