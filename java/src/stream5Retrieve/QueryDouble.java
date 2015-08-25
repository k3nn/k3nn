package stream5Retrieve;

import io.github.htools.lib.Log;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

/**
 *
 * @author jeroen
 */
public class QueryDouble extends Query {

    public static final Log log = new Log(QueryDouble.class);
    String term;
    String term2;

    protected QueryDouble(HashSet<String> terms, String originalquery) {
        super(terms, originalquery);
        Iterator<String> iter = terms.iterator();
        this.term = iter.next();
        this.term2 = iter.next();
    }

    @Override
    public boolean partialMatch(HashSet<String> terms) {
        return terms.contains(term) || terms.contains(term2);
    }

    public boolean fullMatch(HashSet<String> terms) {
        return terms.contains(term) && terms.contains(term2);
    }

    @Override
    public boolean fullMatch(ArrayList<String> terms) {
        return terms.contains(term) && terms.contains(term2);
    }

}
