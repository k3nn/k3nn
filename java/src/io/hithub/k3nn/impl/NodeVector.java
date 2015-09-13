package io.hithub.k3nn.impl;

import io.github.k3nn.Node;
import io.github.htools.fcollection.FHashMapInt;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorEntropy;
import java.util.ArrayList;
import java.util.Set;

/**
 * Node that is extended with a vector of the contained terms
 * @author jeroen
 */
public class NodeVector extends Node {

    static Log log = new Log(NodeVector.class);
    TermVectorEntropy vector;
    
    public NodeVector(long id, int domain, long creationtime, ArrayList<String> terms) {
        super(id, domain, creationtime);
        vector = new TermVectorEntropy(terms);
    }
    
    public NodeVector(long id, int domain, long creationtime, FHashMapInt<String> terms) {
        super(id, domain, creationtime);
        vector = new TermVectorEntropy(terms);
    }
    
    public int countFeatures() {
        return vector.total();
    }  
    
    public TermVectorEntropy getVector() {
        return vector;
    }
    
    @Override
    public Set<String> getTerms() {
        return vector.keySet();
    }
}
