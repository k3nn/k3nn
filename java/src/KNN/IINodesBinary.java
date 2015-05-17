package KNN;

import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.collection.HashMapInt;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.type.Tuple2Comparable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * An inverted index to retrieve nodes that contain a feature.
 * @author jeroen
 */
public class IINodesBinary<N extends NodeM> extends IINodes<N> {

    public static final Log log = new Log(IINodesBinary.class);

    /**
     * @param terms
     * @return a list of Nodes, with the number of terms from the passed list they 
     * appear in
     */
    @Override
    public ArrayMap<Double, N> candidateUrlsCount(N node) {
        double sqrtsize = Math.sqrt(node.countFeatures());
        HashMapInt<N> map = new HashMapInt();
        for (String term : node.getTerms()) {
            HashSet<N> clusters1 = get(term);
            if (clusters1 != null) {
                for (N u : clusters1)
                    map.add(u, 1);
            }
        }
        ArrayMap<Double, N> result = new ArrayMap();
        for (Map.Entry<N, Integer> entry : map.entrySet()) {
            N url = entry.getKey();
            double score = score(node, url, entry.getValue(), sqrtsize);
            result.add(score, entry.getKey());
        }
        return result;
    }
    
    public double score(NodeM a, NodeM b, int count, double sqrta) {
        if (a.domain == b.domain)
            return 0;
        return Score.timeliness(a.getCreationTime(), b.getCreationTime()) * 
               count / (sqrta * Math.sqrt(b.countFeatures()));
    }

    @Override
    public double getMagnitude(NodeM node) {
        return node.countFeatures() * node.countFeatures();
    }
    
    @Override
    public double getFreq(String term) {
        return 1;
    }
}
