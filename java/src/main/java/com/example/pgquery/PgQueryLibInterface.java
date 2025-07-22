package com.example.pgquery;

import com.sun.jna.Library;
import com.sun.jna.Native;

public interface PgQueryLibInterface extends Library {

    PgQueryLibInterface INSTANCE = Native.load("libpg_query.dylib", PgQueryLibInterface.class);

    PgQueryParseResult.ByValue pg_query_parse(String input);

}
