package io.github.k3nn.query;

import io.github.htools.lib.Log;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
/**
 * Query implementation that handles OR functionality by matching the word "or"
 * in queries.
 * @author jeroen
 */
public class QueryOr extends Query {
   public static final Log log = new Log( QueryOr.class );
   ArrayList<Query> queries;
   
   protected QueryOr(ArrayList<Query> queries, String originalquery) {
       super(terms(queries), originalquery);
       this.queries = queries;
       //log.info("subsqueries %d", queries.size());
   }
   
   static HashSet<String> terms(ArrayList<Query> queries) {
       HashSet<String> set = new HashSet();
       for (Query q : queries)
           set.addAll(q.getTerms());
       return set;
   }

    @Override
    public boolean partialMatch(Set<String> terms) {
        for (String t : this.terms) {
           if (terms.contains(t))
               return true;
        }
        return false;
    }

    @Override
    public boolean fullMatch(Set<String> terms) {
        for (Query q : queries) {
           if (q.fullMatch(terms))
               return true;
        }
        return false;
    }

    @Override
    public boolean fullMatch(ArrayList<String> terms) {
        for (Query q : queries) {
           //log.info("fullMatchOr %s %s %s %b", q.getClass().getCanonicalName(), q.getTerms(), terms, q.fullMatch(terms));
           if (q.fullMatch(terms))
               return true;
        }
        return false;
    }

    @Override
    public ArrayList<ArrayList<String>> getQueries() {
        ArrayList<ArrayList<String>> queries = new ArrayList();
        for (Query q : this.queries) {
            queries.addAll(q.getQueries());
        }
        return queries;
    }
}
