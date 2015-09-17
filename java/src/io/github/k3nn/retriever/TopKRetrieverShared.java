package io.github.k3nn.retriever;

import io.github.k3nn.Node;
import io.github.htools.lib.Log;
import java.io.IOException;

/**
 * Shared pools are used when retrieval is distributed over several pools, using
 * different relevance per pool but a shared list of emitted sentences.
 * <p>
 * @author Jeroen
 */
public abstract class TopKRetrieverShared<N extends Node> extends TopKRetriever<N> {

    private static final Log log = new Log(TopKRetrieverShared.class);

    /**
     * Add the model
     *
     * @param otherTopKRetriever
     */
    public void add(TopKRetriever<N> otherTopKRetriever) {
        emittedSentences.addAll(otherTopKRetriever.emittedSentences);
        if (knownwords != otherTopKRetriever.knownwords) {
            knownwords.add(otherTopKRetriever.knownwords);
        }
        relevanceVector.add(otherTopKRetriever.relevanceVector);
    }

    /**
     * Is called whenever the candidate sentence of a streamed cluster is
     * qualified, therefore this should be implemented to capture the results of
     * the retriever.
     */
    @Override
    public abstract void emit(int topic, N u, String title) throws IOException, InterruptedException;

    /**
     * setup a shared pool of emitted sentences and knownwords between multiple
     * retriever instances, which can be used to separate the stream of
     * information and qualify the sentences in each stream against a stream
     * specific relevance model.
     *
     * @param retriever
     */
    public void sharePool(TopKRetrieverShared retriever) {
        this.poolEmittedSentences = retriever.poolEmittedSentences;
        this.knownwords = retriever.knownwords;
    }
}
