package io.github.k3nn.retriever;

import io.github.htools.collection.ArrayMap3;
import io.github.k3nn.impl.NodeSentence;
import io.github.k3nn.Cluster;
import io.github.k3nn.ClusteringGraph;
import io.github.k3nn.Node;
import io.github.htools.collection.HashMapSet;
import io.github.htools.collection.OrderedQueueMap;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorInt;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import io.github.k3nn.query.Query;

/**
 * A TopK Retriever processes an online stream of query matching sentence
 * cluster snapshots, and qualifies whether the candidate sentence (i.e. last
 * sentence added) of such a cluster snapshot qualifies to add to a summary that
 * aims to optimize an F-measure over comprehensiveness and gain. Within these
 * clusters, the most salient sentences are considered to be the core nodes,
 * since these are most similar to other sentences (e.g. in content and
 * proximity in publication time). The general process first checks some general
 * constraints, such as sentence length, whether the sentence is a core sentence
 * (or closely related to the core). A relevance model maintained over all
 * salient sentences that were seen over the previous h hours. In general, a
 * candidate sentence qualifies when it satisfies novelty and relevance
 * constraints.
 * <p>
 * A retriever has parameters for l the maximum sentence length, h the time in
 * hours salient information is kept in the relevance model, g the minimum
 * estimated percentage of novel information and r the rank a sentence has to
 * obtain to qualify when ranked amongst already qualified sentences against the
 * relevance model.
 * <p>
 *
 * @author Jeroen
 * @param <N> Type of Node used
 */
public abstract class TopKRetriever<N extends Node> {

    private static final Log log = new Log(TopKRetriever.class);

    // Map of Similarity Scores for Nodes that were emitted
    protected ArrayList<Sentence> emittedSentences;
    // Used when mixed retrievers are used with a shared pool of emitted sentences
    // Not used by default.
    protected ArrayList<Sentence> poolEmittedSentences;
    // reflects words or word-combinations in qualified sentences that is used
    // to estimate the gain/novelty of a candidate sentence
    protected KnownWords knownwords;
    // a model over the salient information seen in the last h-hours that 
    // reflects the most recent interest in the topic in the news stream
    protected RelevanceVector relevanceVector;
    // tokenizes on non-alphfanumeric chars, remove stopwords, lowercase, no stemmer
    protected DefaultTokenizer tokenizer = getTokenizer();

    // TREC topic id that is returned along with emitted sentences
    protected int topicID;
    // start and end identify the interval between which to look for sentences
    protected long topicStartTime;
    protected long topicEndTime;

    protected boolean watch; // for debugging
    // The topic's query, used for checking if sentenves contain query terms
    protected Query query;

    // requirements to qualify sentences for emission: minRankObtained=r,
    // minInformationGain=g, windowRelevanceModelHours=h, maxSentenceLengthWords=l
    public int minRankObtained = 5;
    public int maxSentenceLengthWords = 20;
    public double minInformationGain = 0.5;
    public double windowRelevanceModelHours = 1;
    protected int windowRelevanceModelSeconds;

    /**
     * Initialize the retriever for a specific topic. For qualification, only
     * sentences with a creation time between topicstart and topicend should be
     * considered
     *
     * @param topicid TREC topicid
     * @param topicstart
     * @param topicend
     * @param query
     */
    public void init(int topicid, long topicstart, long topicend, Query query) throws IOException {
        this.topicID = topicid;
        this.topicStartTime = topicstart;
        this.topicEndTime = topicend;
        this.query = query;
        windowRelevanceModelSeconds = (int) (windowRelevanceModelHours * 60 * 60);

        // by default, a Retriever has it's own pool, therefore the shared pool just
        // points to this.
        emittedSentences = new ArrayList();
        poolEmittedSentences = emittedSentences;

        // initialize the models, the query terms are considered to be permanently
        // known and relevant information, therefore when there is no information seen
        // for h hours, the relevance model should revert to the initial query terms.
        knownwords = createKnownWords();
        relevanceVector = new RelevanceVector(this.query.getTerms());
        knownwords.addKownWords(this.query);
    }

    /**
     * Is called whenever the candidate sentence of a streamed cluster is
     * qualified, therefore this should be implemented to capture the results of
     * the retriever.
     */
    public abstract void emit(int topic, N u, String title) throws IOException, InterruptedException;

    /**
     * Overridable method to create a KnownWords model, so it can be easily
     * replaced by an alternative implementation. By default, a model over
     * 2-word combinations is used.
     *
     * @return
     */
    protected KnownWords createKnownWords() {
        return new KnownWordsCombinations();
    }

    /**
     * Overridable method to create a Tokenizer so it can be easily replaced by
     * an alternative implementation. By default, content is tokenized on non
     * alphanumeric characters, lowercased, stop words removed, but not stemmed.
     */
    public DefaultTokenizer getTokenizer() {
        return ClusteringGraph.getUnstemmedTokenizer();
    }

    /**
     * Inspects the candidate sentence (i.e. last) of a cluster and calls emit
     * when it qualifies. In the process, the relevance model and model of
     * emitted sentences are updated.
     */
    public void qualify(Cluster<N> cluster) throws IOException, InterruptedException {
        // construct a cluster object from the streamclusterwritable
        //Cluster<N> c = createCluster(writable);
        // the last sentence is the only sentence that can qualify for emission
        N candidateNode = (N) cluster.getNodes().get(cluster.getNodes().size() - 1);
        // for the BaseCluster variant, strip nodes that are not reachable form a core node
        cluster.stripCluster();

        // tokenize sentence to list of unique non stop words
        HashSet<String> candidateNodeTermSet = new HashSet(tokenizer.tokenize(candidateNode.getContent()));
        ArrayList<String> candidateNodeTerms = new ArrayList(candidateNodeTermSet);

        // for pre-qualification, the candidate must be a salient sentences (e.g.
        // remain as a member when the cluster s stripped of non salient sentences)
        // the cluster must qualify (e.g. one of its sentences contains all query terms)
        // and the candidate must qualify (e.g. creation time falls within the
        // topic interval and the length must not exceed the maximum sentence length. 
        boolean closeToCore = cluster.getNodes().contains(candidateNode);
        boolean clusterqualifies = coreMatchesQuery(cluster, query);
        boolean notExceedsMaximumLength = notExceedsMaximumLength(candidateNodeTermSet);
        boolean withinTopicInterval = withinTopicInterval(candidateNode);

        if (closeToCore && clusterqualifies && withinTopicInterval && notExceedsMaximumLength) {
            // todo extend relevance model if sentence does not qualify and using restrictions
            Sentence candidate = new Sentence(candidateNode, cluster, candidateNodeTerms);

            // add all other salient sentences except the candidate sentence to
            // the relevanec model
            relevanceVector.addPre(cluster, candidateNode);

            // removes expired sentences from the relevance model
            relevanceVector.removeExpired(candidateNode.getCreationTime());

            // ranks already emitted sentences against the relevance model and
            // determine if the candidate sentence ranks above position minRankObtained
            rankEmittedSentences(poolEmittedSentences);
            candidate.relevance = candidate.vector.cossim(relevanceVector);
            double similarityRankR = (poolEmittedSentences.size() >= minRankObtained)
                    ? poolEmittedSentences.get(minRankObtained - 1).relevance : 0;
            boolean sufficientRank = (poolEmittedSentences.size() < minRankObtained
                    || candidate.relevance >= similarityRankR);

            // determine wether the candidate sentence is sufficiently novel
            boolean sufficientNewInfo = candidate.sufficientNewInformation();

            // determine wether the candidate sentence contain information already seen
            // for ICTIR we used 2, for TREC 2015 we used 1
            boolean sufficientPreviouslySeenInfo = knownwords.getPreviouslySeenInformation(candidateNodeTerms, query, 2);

            if (sufficientPreviouslySeenInfo && sufficientNewInfo && sufficientRank) {
                // the candidate sentence qualifies and is emitted
                emit(topicID, candidateNode, candidateNode.getContent());

                // the sentence is added to the pools of known words and emitted
                // sentences
                knownwords.addKownWords(candidateNodeTerms);
                emittedSentences.add(candidate);
                if (emittedSentences != poolEmittedSentences) {
                    poolEmittedSentences.add(candidate);
                }
            }
            // add the candidate sentence to the relevance model if it is salient
            relevanceVector.addPost(candidateNode);
        }
    }

    /**
     * Represents a candidate sentence that is qualified.
     */
    public class Sentence implements Comparable<Sentence> {

        // although in an array list, this should only contain unique words
        public ArrayList<String> candidateNodeTerms;
        // the cluster snapshot this sentence is a candidate in
        public Cluster<N> cluster;
        // the node representation of this candidate sentence
        public N node;
        // the relevance scored against the most recent relevance model
        public double relevance;
        // vector representation of its contents
        public TermVectorInt vector;

        public Sentence(N node, Cluster<N> cluster, ArrayList<String> terms) {
            this.node = node;
            this.vector = new TermVectorInt(terms);
            candidateNodeTerms = terms;
            this.cluster = cluster;
        }

        /**
         * @return true when sufficiently novel, in the original version when
         * the percentage of 2-word combination in the sentence that co-occur in
         * other salient sentences in the cluster and are not in emitted
         * sentences exceeds the threshold minInformationGain.
         */
        public boolean sufficientNewInformation() {
            //The query terms are discounted because of their very frequent appearance.
            HashSet<String> titleminusquery = new HashSet(candidateNodeTerms);
            titleminusquery.removeAll(query.getTerms());

            double gain = knownwords.estimatePreviouslyUnseenInformation(candidateNodeTerms, cluster, node);
            double gainthreshold = ((titleminusquery.size() * (candidateNodeTerms.size() - 1)) * minInformationGain);
            return titleminusquery.size() > 0 && gain >= gainthreshold;
        }

        @Override
        public int compareTo(Sentence o) {
            return relevance > o.relevance ? -1 : relevance < o.relevance ? 1 : 0;
        }
    }

    /**
     * @param candidateSentenceTerms unique non stop words in sentence
     * @return true if the candidate length does not exceed the maximum length
     */
    public boolean notExceedsMaximumLength(HashSet<String> candidateSentenceTerms) {
        return candidateSentenceTerms.size() <= maxSentenceLengthWords;
    }

    /**
     * @param candidateSentence
     * @return true if the publication time is between the interval given for
     * the topic
     */
    public boolean withinTopicInterval(N candidateSentence) {
        return candidateSentence.getCreationTime() >= topicStartTime
                && candidateSentence.getCreationTime() <= topicEndTime;
    }

    /**
     * @param cluster
     * @param query
     * @return true when a sentence in the cluster contains all query terms
     */
    public boolean coreMatchesQuery(Cluster<N> cluster, Query query) {
        for (N url : cluster.getNodes()) {
            if (query.fullMatch(url.getTerms())) {
                return true;
            }
        }
        return false;
    }

    /**
     * re-assign similarity scores to all emitted sentences using the relevance
     * vector and re-rank the list
     */
    public void rankEmittedSentences(ArrayList<? extends Sentence> list) {
        for (Sentence e : list) {
            e.relevance = e.vector.cossim(relevanceVector);
        }
        Collections.sort(list);
    }

    /**
     * @return creationTime2SentenceIDsentence of the content that is currently
     * in the relevance vector, provided to store snapshots during clustering.
     */
    public ArrayMap3<Long, Long, Collection<String>> getRelevanceEntries() {
        return relevanceVector.getRelevanceEntries();
    }

    /**
     * Relevance Model over a window of most recently seen salient sentences,
     * that is used to rank emitted and candidate sentences to decide whether a
     * candidate sentence obtains a sufficient rank amongst emitted sentences to
     * qualify. Initially the model is seeded with the query, reverting to the
     * query when no salient information was seen for he duration of the window
     * size.
     */
    protected class RelevanceVector extends TermVectorInt {

        class doc {

            long sentenceid;
            Collection<String> content;

            doc(long id, Collection<String> content) {
                sentenceid = id;
                this.content = content;
            }
        }
        // Map<timestamp, Set<terms>>
        OrderedQueueMap<Long, doc> sentenceAddedRelevanceModel = new OrderedQueueMap();

        // list of docs already added to relevance model (so they dont get added twice)
        HashSet<Long> sentenceAddedToRelevanceModel;

        double magnitude = 0;

        /**
         * @param query a set of unique query terms used to seed the relevance
         * model
         */
        public RelevanceVector(Collection<String> query) {
            sentenceAddedToRelevanceModel = new HashSet();
            sentenceAddedRelevanceModel = new OrderedQueueMap();
            add(query);
        }

        /**
         * Added for shared TopKRetrievers, to merge relevance models
         *
         * @param otherRelevanceVector
         */
        public void add(RelevanceVector otherRelevanceVector) {
            super.add(otherRelevanceVector);
            sentenceAddedToRelevanceModel.addAll(otherRelevanceVector.sentenceAddedToRelevanceModel);
            sentenceAddedRelevanceModel.addAll(otherRelevanceVector.sentenceAddedRelevanceModel);
        }

        /**
         * Add the 2-degenerate core nodes (except the candidate) to the
         * relevance model, since this is the information seen before the
         * candidate. The core sentences are checked against previously added
         * sentences to avoid duplicates.
         *
         * @param cluster
         * @param candidateNode
         */
        public void addPre(Cluster<N> cluster, N candidateNode) {
            for (N coreNode : cluster.getCore()) {
                if (coreNode != candidateNode && !sentenceAddedToRelevanceModel.contains(coreNode.getID())) {
                    sentenceAddedToRelevanceModel.add(coreNode.getID());
                    this.add(coreNode.getTerms());
                    this.sentenceAddedRelevanceModel.add(coreNode.getCreationTime(), new doc(coreNode.getID(), coreNode.getTerms()));
                }
            }
        }

        /**
         * Add a (qualified) candidate node to the relevance model.
         *
         * @param candidateNode
         */
        public void addPost(N candidateNode) {
            if (!sentenceAddedToRelevanceModel.contains(candidateNode.getID())) {
                sentenceAddedToRelevanceModel.add(candidateNode.getID());
                this.add(candidateNode.getTerms());
                this.sentenceAddedRelevanceModel.add(candidateNode.getCreationTime(), new doc(candidateNode.getID(), candidateNode.getTerms()));
            }
        }

        /**
         * Used to seed the relevance model with a previously stored snapshot
         *
         * @param creationTime
         * @param documentID
         * @param terms
         */
        public void add(long creationTime, long documentID, Collection<String> terms) {
            this.add(terms);
            this.sentenceAddedRelevanceModel.add(creationTime, new doc(documentID, terms));
            this.sentenceAddedToRelevanceModel.add(documentID);
        }

        /**
         * Based on the currentCreationTime, all information that was added
         * before the start of the window used for the relevance model is
         * removed.
         *
         * @param currentCreationTime the creation time of the current node
         */
        public void removeExpired(long currentCreationTime) {
            currentCreationTime -= windowRelevanceModelSeconds;
            while (sentenceAddedRelevanceModel.size() > 0) {
                Map.Entry<Long, doc> entry = sentenceAddedRelevanceModel.peek();
                if (entry.getKey() < currentCreationTime) {
                    remove(entry.getValue().content);
                    sentenceAddedRelevanceModel.poll();
                } else {
                    break;
                }
            }
        }

        private void remove(Collection<String> terms) {
            for (String term : terms) {
                int freq = get(term);
                if (freq == 1) {
                    remove(term);
                } else {
                    put(term, freq - 1);
                }
            }
            magnitude = 0;
        }

        /**
         * @return true if the model has changed, indicating that emitted nodes
         * should be re-scored.
         */
        public boolean hasChanged() {
            return magnitude == 0;
        }

        /**
         * @return creationTime2SentenceIDsentence of the content that is
         * currently in the relevance vector, provided to store snapshots during
         * clustering.
         */
        protected ArrayMap3<Long, Long, Collection<String>> getRelevanceEntries() {
            ArrayMap3<Long, Long, Collection<String>> entries = new ArrayMap3();
            for (Map.Entry<Long, RelevanceVector.doc> entry : sentenceAddedRelevanceModel) {
                entries.add(entry.getKey(), entry.getValue().sentenceid, entry.getValue().content);
            }
            return entries;
        }
    }

    /**
     * Models the information contained in the emitted sentences, to estimate
     * the gain (or novelty) of candidate sentences. In our experiments, the
     * KnownWordsCombinations model worked best.
     *
     * @param <N>
     */
    public abstract class KnownWords<N extends Node> {

        /**
         * Adds content to KnownWords model
         *
         * @param terms terms that appear in the content to be added
         */
        public abstract void addKownWords(ArrayList<String> terms);

        /**
         * Adds a query to KnownWords model
         *
         * @param query
         */
        public abstract void addKownWords(Query query);

        /**
         * Computes a score [0,1] that reflects the amount of novel information
         * in the candidateSentence, that is supported by salient sentences from
         * a different domain in the cluster, and was not seen in previously
         * emitted sentences.
         *
         * @param terms
         * @param cluster
         * @param candidateSentence
         * @return [0,1] reflecting the amount of novel information.
         */
        public abstract double estimatePreviouslyUnseenInformation(ArrayList<String> terms, Cluster<N> cluster, Node candidateSentence);

        /**
         * @param terms that appear in the candidate sentence
         * @param query
         * @param minimalAmount
         * @return true when at least a minimal amount of information was also
         * seen in the query or previously emitted sentences, to prevent topic
         * drift.
         */
        public abstract boolean getPreviouslySeenInformation(ArrayList<String> terms, Query query, int minimalAmount);

        /**
         * Added to support merging of TopKRetriever models, merges the known
         * word models of two separate retrievers.
         *
         * @param otherKnownWords
         */
        public abstract void add(KnownWords otherKnownWords);
    }

    /**
     * Implementation of KnownWords that counts the fraction of word
     * combinations that co-occur in salient cluster sentences but were not
     * previously seen in an emitted sentences.
     */
    protected class KnownWordsCombinations extends KnownWords<N> {

        protected HashMapSet<String, String> knownwords = new HashMapSet();

        @Override
        public void add(KnownWords other) {
            knownwords.addAll(((KnownWordsCombinations) other).knownwords);
        }

        @Override
        public void addKownWords(Query query) {
            for (ArrayList<String> terms : query.getQueries()) {
                addKownWords(terms);
            }
        }

        @Override
        public void addKownWords(ArrayList<String> terms) {
            for (int i = 0; i < terms.size(); i++) {
                for (int j = 0; j < terms.size(); j++) {
                    if (i != j) {
                        String t1 = terms.get(i);
                        String t2 = terms.get(j);
                        HashSet<String> list = knownwords.get(terms.get(i));
                        if (list == null) {
                            list = new HashSet();
                            knownwords.put(terms.get(i), list);
                        }
                        if (!list.contains(terms.get(j))) {
                            list.add(terms.get(j));
                        }
                    }
                }
            }
        }

        @Override
        public double estimatePreviouslyUnseenInformation(ArrayList<String> terms, Cluster<N> cluster, Node candidateSentence) {
            int support = 0;
            for (int i = 0; i < terms.size(); i++) {
                HashSet<String> emittedWithFirst = knownwords.get(terms.get(i));
                for (int j = i + 1; j < terms.size(); j++) {
                    if (emittedWithFirst == null || !emittedWithFirst.contains(terms.get(j))) {
                        for (Node v : cluster.getNodes()) {
                            if (v.getDomain() != candidateSentence.getDomain() && v.getTerms().contains(terms.get(i)) && v.getTerms().contains(terms.get(j))) {
                                support++;
                                break;
                            }
                        }
                    }
                }
            }
            return support;
        }

        /**
         * @param terms unique terms in the candidate sentence
         * @param query
         * @return number of Word Combinations that appear jointly in an emitted
         * sentence or the query, with at least one query term
         */
        @Override
        public boolean getPreviouslySeenInformation(ArrayList<String> terms, Query query, int e) {
            if (query.fullMatch(terms)) {
                return true;
            }
            for (int i = 0; i < terms.size(); i++) {
                if (query.getTerms().contains(terms.get(i))) {
                    HashSet<String> list = knownwords.get(terms.get(i));
                    for (int j = 0; j < terms.size(); j++) {
                        if (j != i) {
                            if (list.contains(terms.get(j))) {
                                if (--e < 1) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            return false;
        }
    }

    /**
     * Implementation of KnownWords that counts the fraction of unigrams that
     * co-occur in salient cluster sentences but were not previously seen in an
     * emitted sentences.
     */
    protected class KnownWordsUnigram extends KnownWords<N> {

        public KnownWordsUnigram() {
        }

        HashSet<String> knownwords = new HashSet();

        @Override
        public void add(KnownWords other) {
            knownwords.addAll(((KnownWordsUnigram) other).knownwords);
        }

        @Override
        public void addKownWords(Query query) {
            for (ArrayList<String> terms : query.getQueries()) {
                addKownWords(terms);
            }
        }

        @Override
        public void addKownWords(ArrayList<String> terms) {
            knownwords.addAll(terms);
        }

        @Override
        public double estimatePreviouslyUnseenInformation(ArrayList<String> terms, Cluster<N> cluster, Node candidateSentence) {
            int support = 0;
            for (String term : terms) {
                if (!knownwords.contains(term)) {
                    for (Node v : cluster.getNodes()) {
                        if (v.getDomain() != candidateSentence.getDomain() && v.getTerms().contains(term)) {
                            support++;
                        }
                    }
                }
            }
            return support;
        }

        /**
         * @param terms unique terms in the candidate sentence
         * @param query
         * @return number of Word Combinations that appear jointly in an emitted
         * sentence or the query, with at least one query term
         */
        @Override
        public boolean getPreviouslySeenInformation(ArrayList<String> terms, Query query, int minimalInformation) {
            for (String term : terms) {
                if (knownwords.contains(term)) {
                    if (--minimalInformation < 1) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
