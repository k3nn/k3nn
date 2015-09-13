package io.hithub.k3nn.impl;

import io.github.k3nn.ClusteringGraph;
import io.github.htools.lib.Log;
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;

/**
 * Node that memorizes the terms, collection uuid and sentence number.
 * @author jeroen
 */
public class NodeMagnitude extends NodeSentence {

    static Log log = new Log(NodeMagnitude.class);
    public double magnitude;

    public NodeMagnitude(ClusteringGraph<NodeMagnitude> stream, long id, int domain, String title, HashSet<String> features, long creationtime, UUID uuid, int sentence) {
        super(id, domain, title, features, creationtime, uuid, sentence);
        magnitude = ((NodeStoreIIIDF)stream.iinodes).getMagnitude(this);
    }
    
    /**
     * @return collection document id, consisting of creation time + uuid;
     */
    public String getDocumentID() {
        return this.getCreationTime() + "-" + uuid.toString().replace("-", "");
    }
}
