package io.github.k3nn.retriever;

import io.github.k3nn.Node;
import io.github.htools.lib.Log;
import java.io.IOException;

/**
 * Shared pools are used when retrieval is distributed over several pools, using 
 * different relevance per pool but a shared list of emitted sentences.
 * <p/>
 * @author Jeroen
 */
public abstract class TopKRetrieverShared<N extends Node> extends TopKRetriever<N> {

    private static final Log log = new Log(TopKRetrieverShared.class);

    /**
     * Add the model 
     * @param other 
     */
    public void add(TopKRetriever<N> other) {
        emittedSentences.addAll(other.emittedSentences);
        if (knownwords != other.knownwords) {
            knownwords.add(other.knownwords);
        }
        relevanceVector.add(other.relevanceVector);
    }

    /**
     * Is called whenever the candidate sentence of a streamed cluster is qualified,
     * therefore this should be implemented to capture the results of the retriever.
     */
    public abstract void emit(int topic, N u, String title) throws IOException, InterruptedException;

    public void sharePool(TopKRetrieverShared retriever) {
        this.poolEmittedSentences = retriever.poolEmittedSentences;
        this.knownwords = retriever.knownwords;
    }
}
