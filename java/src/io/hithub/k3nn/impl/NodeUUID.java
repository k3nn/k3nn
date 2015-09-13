package io.hithub.k3nn.impl;

import io.github.k3nn.Node;
import io.github.htools.lib.Log;
import io.github.htools.type.Long128;
import java.util.Collection;

/**
 *
 * @author jeroen
 */
public class NodeUUID extends Node {

    static Log log = new Log(NodeUUID.class);
    long uuid1;
    long uuid2;
    protected int featureCount;
    
    public NodeUUID(long id, int domain, long creationtime, Collection<String> features, Long128 uuid) {
        this(id, domain, creationtime, features.size(), uuid);
    }

    public NodeUUID(long id, int domain, long creationtime, int features, Long128 uuid) {
        super(id, domain, creationtime);
        uuid1 = uuid.getUUID().getMostSignificantBits();
        uuid2 = uuid.getUUID().getLeastSignificantBits();
        this.featureCount = features;
    }
    
    public int countFeatures() {
        return featureCount;
    }
    
    public Long128 getUUID() {
        return new Long128(uuid1, uuid2);
    }
}
