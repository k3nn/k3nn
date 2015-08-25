package stream5Retrieve;

import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.lib.Log;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

/**
 *
 * @author jeroen
 */
public abstract class Query {

    public static final Log log = new Log(Query.class);
    public String originalquery;
    public HashSet<String> terms;
    
    protected Query(HashSet<String> terms, String originalquery) {
        this.terms = terms;
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
    
    public HashSet<String> getTerms() {
        return terms;
    }
    
    public abstract boolean partialMatch(HashSet<String> terms);

    public abstract boolean fullMatch(HashSet<String> terms);
    
    public abstract boolean fullMatch(ArrayList<String> terms);

}
