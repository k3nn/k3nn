package io.hithub.k3nn.impl;

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
public class NodeStoreIIBinary<N extends NodeM> extends IINodes<N> {

    public static final Log log = new Log(NodeStoreIIBinary.class);

    /**
     * @param terms
     * @return a list of Nodes, with the number of terms from the passed list
     * they appear in
     */
    @Override
    public ArrayMapDouble<N> addGetList(N newNode, Collection<String> terms) {
        double sqrtSizeA = Math.sqrt(newNode.countFeatures());
        FHashMapInt<N> node2TermCount = new FHashMapInt();
        for (String term : terms) {
            ObjectArrayList<N> invertedList = get(term);
            if (invertedList != null) {
                for (N u : invertedList) {
                    node2TermCount.add(u, 1);
                }
            }
        }
        ArrayMapDouble<N> result = new ArrayMapDouble();
        for (Object2IntMap.Entry<N> entry : node2TermCount.object2IntEntrySet()) {
            N node = entry.getKey();
            int termCount = entry.getIntValue();
            double score = score(node, newNode, termCount, sqrtSizeA);
            if (score > 0) {
                result.add(score, entry.getKey());
            }
        }
        add(newNode, terms);
        return result;
    }

    public double score(NodeM a, NodeM b, int termCount, double sqrtSizeA) {
        if (a.domain == b.domain) {
            return 0;
        }
        return Score.timeliness(a.getCreationTime(), b.getCreationTime())
                * termCount / (sqrtSizeA * Math.sqrt(b.countFeatures()));
    }
}
