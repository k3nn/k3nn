package KNN;

import io.github.repir.tools.lib.Log;
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;

/**
 * Node that memorizes the terms, collection uuid and sentence number.
 * @author jeroen
 */
public class NodeMagnitude extends NodeD {

    static Log log = new Log(NodeMagnitude.class);
    public double magnitude;

    public NodeMagnitude(Stream<NodeMagnitude> stream, long id, int domain, String title, HashSet<String> features, long creationtime, UUID uuid, int sentence) {
        super(id, domain, title, features, creationtime, uuid, sentence);
        magnitude = stream.iinodes.getMagnitude(this);
    }
    
    /**
     * @return collection document id, consisting of creation time + uuid;
     */
    public String getDocumentID() {
        return this.getCreationTime() + "-" + uuid.toString().replace("-", "");
    }
}
