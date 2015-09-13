package io.hithub.k3nn.impl;

import io.github.htools.lib.Log;
import java.util.HashSet;
import java.util.UUID;

/**
 * Node that memorizes the terms, termvector, collection uuid and sentence number.
 * @author jeroen
 */
public class NodeSentence extends NodeTerms {

    static Log log = new Log(NodeSentence.class);
    public UUID uuid;
    public int sentence;

    public NodeSentence(long id, int domain, String title, HashSet<String> features, long creationtime, UUID uuid, int sentence) {
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
