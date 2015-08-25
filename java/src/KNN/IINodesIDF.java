package KNN;

import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.HashMapDouble;
import io.github.htools.collection.HashMapInt;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import io.github.htools.type.Tuple2Comparable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

/**
 * An inverted index to retrieve nodes that contain a feature.
 * @author jeroen
 */
public class IINodesIDF<N extends NodeMagnitude> extends IINodes<N> {

    public static final Log log = new Log(IINodesIDF.class);
    public HashMapDouble<String> idf2;
    public Double missing;

    public IINodesIDF(int collectioncount, HashMapInt<String> df) {
        idf2 = new HashMapDouble();
        for (Map.Entry<String, Integer> entry : df.entrySet()) {
            double idf = MathTools.log2(collectioncount / (1.0 + entry.getValue()));
            idf2.put(entry.getKey(), idf * idf);
        }
        missing = MathTools.log2(collectioncount) * MathTools.log2(collectioncount);
    }
    
    public Double getIDF2(String term) {
        Double termidf = idf2.get(term);
        if (termidf == null)
            return missing;
        return termidf;
    }
    
    /**
     * @param terms
     * @return a list of Nodes, with the number of terms from the passed list they 
     * appear in
     */
    public ArrayMap<Double, N> candidateUrlsCount(N node) {
        HashMapDouble<N> map = new HashMapDouble();
        for (String term : node.getTerms()) {
            HashSet<N> nodes = this.get(term);
            if (nodes != null) {
                for (N u : nodes)
                    map.add(u, getIDF2(term));
            }
        }
        ArrayMap<Double, N> result = new ArrayMap();
        for (Map.Entry<N, Double> entry : map.entrySet()) {
            N url = entry.getKey();
            double cosine = entry.getValue() / (node.magnitude * url.magnitude);
            result.add(cosine, entry.getKey());
        }
        return result;
    }
    
    @Override
    public double getMagnitude(N node) {
        double magnitude = 0;
        for (String term : node.getTerms()) {
            magnitude += getIDF2(term);
        }
        return Math.sqrt(magnitude);
    }
    
    @Override
    public double getFreq(String term) {
        return getIDF2(term);
    }
    
}
