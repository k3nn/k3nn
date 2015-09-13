package io.github.k3nn.query;

import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.fcollection.FHashSet;
import io.github.htools.lib.Log;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents the Query for a topic, that can match itself to text content.
 * @author jeroen
 */
public abstract class Query {

    public static final Log log = new Log(Query.class);
    public String originalquery;
    public FHashSet<String> terms;
    
    protected Query(Set<String> terms, String originalquery) {
        this.terms = terms instanceof FHashSet?(FHashSet)terms:new FHashSet(terms);
        this.originalquery = originalquery;
    }

    public static Query create(DefaultTokenizer tokenizer, String query) {
        if (query.contains(" or ")) {
            ArrayList<Query> queries = new ArrayList();
            for (String singlequery : query.split(" or ")) {
                queries.add(Query.create(tokenizer, singlequery));
            }
            return new QueryOr(queries, query);
        }
        HashSet<String> queryterms = new HashSet(tokenizer.tokenize(query));
        if (queryterms.size() == 1) {
            return new QuerySingle(queryterms, query);
        } else if (queryterms.size() == 2) {
            return new QueryDouble(queryterms, query);
        }
        return new QueryMulti(queryterms, query);
    }

    public ArrayList<ArrayList<String>> getQueries() {
        ArrayList<ArrayList<String>> queries = new ArrayList();
        queries.add(new ArrayList(terms));
        return queries;
    }
    
    public String getOriginalQuery() {
        return originalquery;
    }
    
    public Set<String> getTerms() {
        return terms;
    }
    
    public abstract boolean partialMatch(Set<String> terms);

    public abstract boolean fullMatch(Set<String> terms);
    
    public abstract boolean fullMatch(ArrayList<String> terms);

}
