package stream5Retrieve;

import io.github.htools.lib.Log;
import java.util.ArrayList;
import java.util.HashSet;

/**
 *
 * @author jeroen
 */
public class QuerySingle extends Query {

    public static final Log log = new Log(QuerySingle.class);
    String term;

    protected QuerySingle(HashSet<String> terms, String originalquery) {
        super(terms, originalquery);
        this.term = terms.iterator().next();
    }

    @Override
    public boolean partialMatch(HashSet<String> terms) {
        return terms.contains(term);
    }

    @Override
    public boolean fullMatch(HashSet<String> terms) {
        return terms.contains(term);
    }

    @Override
    public boolean fullMatch(ArrayList<String> terms) {
        return terms.contains(term);
    }

}
