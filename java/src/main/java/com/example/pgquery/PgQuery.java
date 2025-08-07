package com.example.pgquery;
import com.sun.jna.Native;


public class PgQuery {

    public static String parse(String query, String libraryPath) throws Exception {
        var lib = PgQueryLibInterface.load(libraryPath);
        var res = lib.pg_query_parse(query);

        if (res.error == null) {
            return res.parse_tree;
        } else {
            throw new Exception(res.error.message);
        }
    }

    public static PgQueryLibInterface load(String libraryPath) {
        return Native.load(libraryPath, PgQueryLibInterface.class);
    }
}
