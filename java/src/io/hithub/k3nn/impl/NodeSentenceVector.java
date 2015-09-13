package io.hithub.k3nn.impl;

import io.github.htools.fcollection.FHashMapInt;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorEntropy;
import java.util.ArrayList;
import java.util.UUID;

/**
 *
 * @author jeroen
 */
public class NodeSentenceVector extends NodeVector {

    static Log log = new Log(NodeSentenceVector.class);
    public UUID uuid;
    public int sentence;
    public String content;

    public NodeSentenceVector(long id, int domain, String content, long creationtime, ArrayList<String> terms, UUID uuid, int sentence) {
        super(id, domain, creationtime, terms);
        this.uuid = uuid;
        this.sentence = sentence;
        this.content = content;
    }
    
    public NodeSentenceVector(long id, int domain, String content, long creationtime, FHashMapInt<String> terms, UUID uuid, int sentence) {
        super(id, domain, creationtime, terms);
        this.uuid = uuid;
        this.sentence = sentence;
        this.content = content;
    }
    
    public String getDocumentID() {
        return this.getCreationTime() + "-" + uuid.toString().replace("-", "");
    }
    
    @Override
    public String getContent() {
        return content;
    }
}
