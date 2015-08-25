package KNN;

import io.github.htools.lib.Log;
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;

/**
 * Node that memorizes the terms, collection uuid and sentence number.
 * @author jeroen
 */
public class NodeD extends NodeW {

    static Log log = new Log(NodeD.class);
    public UUID uuid;
    public int sentence;

    public NodeD(long id, int domain, String title, HashSet<String> features, long creationtime, UUID uuid, int sentence) {
        super(id, domain, title, features, creationtime);
        this.uuid = uuid;
        this.sentence = sentence;
    }
    
    
    
    /**
     * @return collection document id, consisting of creation time + uuid;
     */
    public String getDocumentID() {
        return this.getCreationTime() + "-" + uuid.toString().replace("-", "");
    }
}
