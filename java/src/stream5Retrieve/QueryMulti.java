package stream5Retrieve;

import io.github.htools.lib.Log;
import java.util.ArrayList;
import java.util.HashSet;

/**
 *
 * @author jeroen
 */
public class QueryMulti extends Query {

    public static final Log log = new Log(QueryMulti.class);

    protected QueryMulti(HashSet<String> query, String originalquery) {
        super(query, originalquery);
    }

    @Override
    public boolean partialMatch(HashSet<String> terms) {
        for (String s : terms) {
            if (terms.contains(s)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean fullMatch(HashSet<String> terms) {
        for (String s : this.terms) {
            log.trace("fullMatch %s %s %b", getTerms(), s, terms.contains(s));
            if (!terms.contains(s)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean fullMatch(ArrayList<String> terms) {
        for (String s : this.terms) {
            if (!terms.contains(s)) {
                return false;
            }
        }
        return true;
    }
}
