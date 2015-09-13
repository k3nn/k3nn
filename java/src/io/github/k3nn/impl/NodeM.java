package io.github.k3nn.impl;

import io.github.k3nn.Node;
import io.github.htools.lib.Log;
import java.util.Collection;

/**
 * Memory efficient extension of node, which holds the number of terms to allow
 * computation of cosine similarity using 
 * @author jeroen
 */
public class NodeM extends Node {

    static Log log = new Log(NodeM.class);
    protected int featureCount;
    
    public NodeM(long id, int domain, long creationtime, Collection<String> features) {
        this(id, domain, creationtime, features.size());
    }

    public NodeM(long id, int domain, long creationtime, int features) {
        super(id, domain, creationtime);
        this.featureCount = features;
    }
    
    public int countFeatures() {
        return featureCount;
    }  
}
