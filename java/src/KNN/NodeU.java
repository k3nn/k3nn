package KNN;

import io.github.htools.lib.Log;
import io.github.htools.type.Long128;
import java.util.Collection;

/**
 *
 * @author jeroen
 */
public class NodeU extends Node {

    static Log log = new Log(NodeU.class);
    long uuid1;
    long uuid2;
    protected int featureCount;
    
    public NodeU(long id, int domain, long creationtime, Collection<String> features, Long128 uuid) {
        this(id, domain, creationtime, features.size(), uuid);
    }

    public NodeU(long id, int domain, long creationtime, int features, Long128 uuid) {
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
