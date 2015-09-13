package io.github.k3nn.impl;

import io.github.k3nn.ClusteringGraph;
import io.github.htools.lib.Log;
import java.util.HashSet;

/**
 * ClusteringGraph that automatically purges after each day or 100000 additions.
 * @author Jeroen
 */
public class ClusteringGraphPurgable<N extends NodeSentence> extends ClusteringGraph<N> {

    public static Log log = new Log(ClusteringGraphPurgable.class);

    final long secondsPerDay = 24 * 60 * 60;
    long threshold = 0;
    int count = 0;

    public ClusteringGraphPurgable() {
        super();
    }

    /**
     * add sentence to the clustering graph, and write the cluster to the output
     * file if the sentence is clustered and is a candidate sentence.
     */
    @Override
    public void addSentence(N sentence, HashSet<String> terms, boolean isCandidate) {
        super.addSentence(sentence, terms, isCandidate);
        checkPurge(sentence.getCreationTime());
    }

    public void checkPurge(long creationtime) {
        if (creationtime > threshold || count++ > 100000) {
            if (threshold == 0) {
                threshold = ((creationtime / secondsPerDay) + 5) * secondsPerDay;
            } else {
                super.purge(creationtime);
                threshold += secondsPerDay;
            }
            count = 0;
        }
    }

    /**
     * write clustered candidate sentence to the clusterFile
     */
    public void clusteredCandidateSentence(NodeSentence candidateSentence) {
    }
}
