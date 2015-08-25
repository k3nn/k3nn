package KNN;

import io.github.htools.lib.Log;
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;

/**
 * Node that memorizes the terms, collection uuid and sentence number.
 * @author jeroen
 */
public class NodeW extends NodeTitle {

    static Log log = new Log(NodeW.class);
    public HashSet<String> terms;

    public NodeW(long id, int domain, String title, HashSet<String> features, long creationtime) {
        super(id, domain, title, features, creationtime);
        this.terms = features;
    }
        
    @Override
    public HashSet<String> getTerms() {
        return terms;
    }
}
