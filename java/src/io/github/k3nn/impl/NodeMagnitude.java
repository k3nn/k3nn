package io.github.k3nn.impl;

import io.github.k3nn.ClusteringGraph;
import io.github.htools.lib.Log;
import java.util.HashSet;
import java.util.UUID;

/**
 * Extension to NodeSentence that caches the magnitude of its TF-IDF vector.
 * @author jeroen
 */
public class NodeMagnitude extends NodeSentence {

    static Log log = new Log(NodeMagnitude.class);
    public double magnitude;

    public NodeMagnitude(ClusteringGraph<NodeMagnitude> stream, long id, int domain, String title, HashSet<String> features, long creationtime, UUID uuid, int sentence) {
        super(id, domain, title, features, creationtime, uuid, sentence);
        magnitude = ((NodeStoreIIIDF)stream.nodeStore).getMagnitude(this);
    }
}
