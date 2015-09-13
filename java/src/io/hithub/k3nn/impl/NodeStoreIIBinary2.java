package io.hithub.k3nn.impl;

import io.github.k3nn.Cluster;
import io.github.k3nn.IINodes;
import io.github.k3nn.Score;
import io.github.htools.collection.ArrayMapDouble;
import io.github.htools.fcollection.FHashMapInt;
import io.github.htools.lib.Log;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.Collection;

/**
 * An inverted index to retrieve nodes that contain a feature.
 *
 * @author jeroen
 */
public class NodeStoreIIBinary2<N extends NodeM> extends IINodes<N> {

    public static final Log log = new Log(NodeStoreIIBinary2.class);

    /**
     * @param terms
     * @return a list of Nodes, with the number of terms from the passed list
     * they appear in
     */
    @Override
    public ArrayMapDouble<N> addGetList(N newNode, Collection<String> terms) {
        double sqrtSizeA = Math.sqrt(newNode.countFeatures());
        FHashMapInt<N> node2TermCount = new FHashMapInt(10000);
        for (String term : terms) {
            ObjectArrayList<N> invertedList = get(term);
            if (invertedList != null && (invertedList.size() < nodes.size() / 10 || invertedList.size() < 100)) {
                for (N node : invertedList) {
                    node2TermCount.add(node);
                }
            }
        }
        ArrayMapDouble<N> candidates = new ArrayMapDouble();
        for (Object2IntMap.Entry<N> entry : node2TermCount.object2IntEntrySet()) {
            if (entry.getIntValue() > 1) {
                N node = entry.getKey();
                int termCount = entry.getIntValue();
                double score = score(newNode, node, termCount, sqrtSizeA);
                if (score > 0) {
                    candidates.add(score, entry.getKey());
                }
            }
        }
        if (candidates.size() < Cluster.K) {
            for (Object2IntMap.Entry<N> entry : node2TermCount.object2IntEntrySet()) {
                if (entry.getIntValue() == 1) {
                    N node = entry.getKey();
                    double score = score(newNode, node, entry.getIntValue(), sqrtSizeA);
                    if (score > 0) {
                        candidates.add(score, entry.getKey());
                    }
                }
            }
        }
        add(newNode, terms);
        return candidates;
    }

    public double score(NodeM a, NodeM b, int count, double sqrtSizeA) {
        if (a.domain == b.domain) {
            return 0;
        }
        return Score.timeliness(a.getCreationTime(), b.getCreationTime())
                * count / (sqrtSizeA * Math.sqrt(b.countFeatures()));
    }
}
