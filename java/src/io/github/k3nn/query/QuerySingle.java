package io.github.k3nn.query;

import io.github.htools.lib.Log;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Implementation for single term queries
 * @author jeroen
 */
public class QuerySingle extends Query {

    public static final Log log = new Log(QuerySingle.class);
    String term;

    protected QuerySingle(Set<String> terms, String originalquery) {
        super(terms, originalquery);
        this.term = terms.iterator().next();
    }

    @Override
    public boolean partialMatch(Set<String> terms) {
        return terms.contains(term);
    }

    @Override
    public boolean fullMatch(Set<String> terms) {
        return terms.contains(term);
    }

    @Override
    public boolean fullMatch(ArrayList<String> terms) {
        return terms.contains(term);
    }

}
