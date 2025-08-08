package sql2dsql.pgquery;

import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.ptr.PointerByReference;

import sql2dsql.pgquery.PgQueryError;

import java.util.Arrays;
import java.util.List;

public class PgQueryParseResult extends Structure {

    public static class ByReference extends PgQueryParseResult implements Structure.ByReference {
    }

    public static class ByValue extends PgQueryParseResult implements Structure.ByValue {
    }

    public PgQueryParseResult() {
    }

    protected PgQueryParseResult(Pointer p) {
        super(p);
    }

    public String parse_tree;
    public String stderr_buffer;
    public PgQueryError.ByReference error;


    @Override
    protected List<String> getFieldOrder() {
        return Arrays.asList("parse_tree", "stderr_buffer", "error");
    }
    
}
