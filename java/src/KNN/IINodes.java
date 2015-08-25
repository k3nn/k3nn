package KNN;

import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.HashMapInt;
import io.github.htools.lib.Log;
import io.github.htools.type.Tuple2Comparable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * An inverted index to retrieve nodes that contain a feature.
 * @author jeroen
 */
public abstract class IINodes<N extends Node> extends HashMap<String, HashSet<N>> {

    public static final Log log = new Log(IINodes.class);

    /**
     * Add a node to the postings list of a term
     * @param term
     * @param node 
     */
    public void add(String term, N node) {
        HashSet<N> list = get(term);
        if (list == null) {
            list = new HashSet();
            put(term, list);
        }
        list.add(node);
    }
    
    public abstract double getMagnitude(N node);
    
    public abstract double getFreq(String term);

    /**
     * Add a node to the postings lists of the terms
     * @param node
     * @param terms 
     */
    public void add(N node, Collection<String> terms) {
        //log.info("add %d", url.getID());
        for (String term : terms) {
            add(term, node);
        }
    }

    /**
     * @param terms
     * @return a list of Nodes, with the number of terms from the passed list they 
     * appear in
     */
    public abstract ArrayMap<Double, N> candidateUrlsCount(N node);
    
    public void purge(long datetime) {
        Iterator<HashSet<N>> iter = this.values().iterator();
        while (iter.hasNext()) {
            HashSet<N> hs = iter.next();
            Iterator<N> iter2 = hs.iterator();
            while (iter2.hasNext()) {
                N node = iter2.next();
                if (node.creationtime < datetime) {
                    iter2.remove();
                }
            }
            if (hs.size() == 0) {
                iter.remove();
            }
        }
    }
}
