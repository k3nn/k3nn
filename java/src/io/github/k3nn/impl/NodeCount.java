package io.github.k3nn.impl;

import io.github.k3nn.Node;
import io.github.htools.lib.Log;
import java.util.Collection;

/**
 * Memory efficient extension of node, which holds the number of terms to allow
 * computation of cosine similarity (using term frequency information from 
 * inverted lists)
 * 
 * @author jeroen
 */
public class NodeCount extends Node {

    static Log log = new Log(NodeCount.class);
    protected int termCount;
    
    public NodeCount(long id, int domain, long creationtime, Collection<String> features) {
        this(id, domain, creationtime, features.size());
    }

    public NodeCount(long id, int domain, long creationtime, int features) {
        super(id, domain, creationtime);
        this.termCount = features;
    }
    
    public int countFeatures() {
        return termCount;
    }  
}
