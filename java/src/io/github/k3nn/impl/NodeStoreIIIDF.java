package io.github.k3nn.impl;

import io.github.k3nn.IINodes;
import io.github.k3nn.Score;
import io.github.htools.collection.ArrayMapDouble;
import io.github.htools.collection.HashMapDouble;
import io.github.htools.collection.HashMapInt;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * An inverted index to retrieve nodes that contain a feature.
 *
 * @author jeroen
 */
public class NodeStoreIIIDF<N extends NodeMagnitude> extends IINodes<N> {

    public static final Log log = new Log(NodeStoreIIIDF.class);
    // IDF * IDF for terms in Wikipedia
    public HashMapDouble<String> idf2;
    // IDF * IDF for missing terms
    public Double missing;

    public NodeStoreIIIDF(int collectioncount, HashMapInt<String> term2DocumentFrequency) {
        idf2 = new HashMapDouble();
        for (Map.Entry<String, Integer> entry : term2DocumentFrequency.entrySet()) {
            int documentFrequency = entry.getValue();
            double idf = MathTools.log2(collectioncount / (1.0 + documentFrequency));
            idf2.put(entry.getKey(), idf * idf);
        }
        missing = MathTools.log2(collectioncount) * MathTools.log2(collectioncount);
    }

    /**
     * @param term
     * @return IDF * IDF for given term
     */
    public Double getIDF2(String term) {
        Double termidf = idf2.get(term);
        if (termidf == null) {
            return missing;
        }
        return termidf;
    }

    /**
     * @param terms
     * @return a list of Nodes, with the number of terms from the passed list
     * they appear in
     */
    public ArrayMapDouble<N> addGetList(N newNode, Collection<String> terms) {
        HashMapDouble<N> node2TermCount = new HashMapDouble();
        for (String term : newNode.getTerms()) {
            ObjectArrayList<N> invertedList = get(term);
            if (invertedList != null) {
                for (N node : invertedList) {
                    node2TermCount.add(node, getIDF2(term));
                }
            }
        }
        ArrayMapDouble<N> score2Node = new ArrayMapDouble();
        for (Map.Entry<N, Double> entry : node2TermCount.entrySet()) {
            N node = entry.getKey();
            double cosine = entry.getValue() / (newNode.magnitude * node.magnitude);
            score2Node.add(cosine, entry.getKey());
        }
        add(newNode, terms);
        return score2Node;
    }

    public double getMagnitude(N node) {
        double magnitude = 0;
        for (String term : node.getTerms()) {
            magnitude += getIDF2(term);
        }
        return Math.sqrt(magnitude);
    }

    public double getFreq(String term) {
        return getIDF2(term);
    }

    public double score(NodeMagnitude a, NodeMagnitude b, int count, double sqrta) {
        if (a.domain == b.domain) {
            return 0;
        }
        double dotproduct = 0;
        if (a.terms.size() < b.terms.size()) {
            for (String term : a.terms) {
                if (b.terms.contains(term)) {
                    dotproduct += getFreq(term);
                }
            }
        } else {
            for (String term : b.terms) {
                if (a.terms.contains(term)) {
                    dotproduct += getFreq(term);
                }
            }
        }
        return Score.timeliness(a.getCreationTime(), b.getCreationTime())
                * dotproduct / (a.magnitude * b.magnitude);
    }

}
