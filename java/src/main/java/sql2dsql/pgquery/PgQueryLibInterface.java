package sql2dsql.pgquery;

import com.sun.jna.Library;
import com.sun.jna.Native;

public interface PgQueryLibInterface extends Library {


    static PgQueryLibInterface load(String libraryPath) {
        return Native.load(libraryPath, PgQueryLibInterface.class);
    }

    PgQueryParseResult.ByValue pg_query_parse(String input);

}
