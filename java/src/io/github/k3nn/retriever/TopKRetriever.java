package io.github.k3nn.retriever;

import io.github.k3nn.Cluster;
import io.github.k3nn.Edge;
import io.github.k3nn.ClusteringGraph;
import io.github.k3nn.Node;
import io.github.k3nn.impl.NodeSentence;
import io.github.k3nn.Cluster;
import io.github.k3nn.ClusteringGraph;
import io.github.k3nn.Edge;
import io.github.k3nn.Node;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.ArrayMap3;
import io.github.htools.collection.HashMapInt;
import io.github.htools.collection.HashMapSet;
import io.github.htools.collection.OrderedQueueMap;
import io.github.htools.io.Datafile;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.fcollection.FHashSet;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorInt;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
 * <p/>
 * A retriever has parameters for l the maximum sentence length, h the time in
 * hours salient information is kept in the relevance model, g the minimum
 * estimated percentage of novel information and r the rank a sentence has to
 * obtain to qualify when ranked amongst already qualified sentences against the
 * relevance model.
 * <p/>
 *
 * @author Jeroen
 * @param <N>
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

    // TREC topic, start and end identify the interval between which to look for
    // sentences
    protected long topicStartTime;
    protected long topicEndTime;
    protected int topicID;

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
     * Initialize streaming sentence clusters and qualifying sentences
     *
     * @param topicid TREC topicid
     * @param topicstart
     * @param topicend
     * @param query
     */
    public void init(int topicid, long topicstart, long topicend, Query query) throws IOException {
        // set topic specific information. For qualification, only senteneces with
        // a creation time between topicstart and topicend should be considered.
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
        knownwords.addKownWordCombinations(this.query);
    }

//    /**
//     * Add the model 
//     * @param other 
//     */
//    public void add(TopKRetriever<N> other) {
//        emittedSentences.addAll(other.emittedSentences);
//        if (knownwords != other.knownwords) {
//            knownwords.add(other.knownwords);
//        }
//        relevanceVector.add(other.relevanceVector);
//    }

    /**
     * Is called whenever the candidate sentence of a streamed cluster is qualified,
     * therefore this should be implemented to capture the results of the retriever.
     */
    public abstract void emit(int topic, N u, String title) throws IOException, InterruptedException;

    /**
     * Overridable method to create a KnownWords model, so it can be easily replaced
     * by an alternative implementation. By default, a model over 2-word combinations
     * is used.
     * @return 
     */
    protected KnownWords createKnownWords() {
        return new KnownWordsCombinations();
    }

    /**
     * Overridable method to create a Tokenizer so it can be easily replaced 
     * by an alternative implementation. By default, content is tokenized on 
     * non alphanumeric characters, lowercased, stop words removed, but not stemmed.
     */
    public DefaultTokenizer getTokenizer() {
        return ClusteringGraph.getUnstemmedTokenizer();
    }

    /**
     * Inspects the candidate sentence (i.e. last) of a cluster and calls emit
     * when it qualifies. In the process, the relevance model and model of emitted
     * sentences are updated.
     */
    public void qualify(Cluster<N> cluster) throws IOException, InterruptedException {
        // construct a cluster object from the streamclusterwritable
        //Cluster<N> c = createCluster(writable);
        // the last sentence is the only sentence that can qualify for emission
        N candidateNode = (N) cluster.getNodes().get(cluster.getNodes().size() - 1);
        // for the BaseCluster variant, strip nodes that are not connected back to
        // returned result is a shallow clone of the original cluster
        cluster.stripCluster();

        // tokenize sentence to list of unique non stop words
        HashSet<String> candidateNodeTermSet = new HashSet(tokenizer.tokenize(candidateNode.getContent()));
        ArrayList<String> candidateNodeTerms = new ArrayList(candidateNodeTermSet);

        // for pre-qualification, the candidate must be a salient sentences (e.g.
        // remain as a member when the cluster s stripped of non salient sentences)
        // the cluster must qualify (e.g. one of its sentences contains all query terms)
        // and the candidate must qualify (e.g. creation time falls within the
        // topic interval and the length must not exceed the maximum l. 
        boolean contains = cluster.getNodes().contains(candidateNode);
        boolean clusterqualifies = qualifies(cluster, query);
        boolean nodeQualifies = qualifyLength(candidateNode, candidateNodeTermSet);

        if (contains && clusterqualifies && nodeQualifies) { // todo extend relevance model if sentence does not qualify and using restrictions
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
            double similarityRankR = (poolEmittedSentences.size() >= minRankObtained) ? poolEmittedSentences.get(minRankObtained - 1).relevance : 0;
            boolean rankqualifies = (poolEmittedSentences.size() < minRankObtained || candidate.relevance >= similarityRankR);

            // determine wether the candidate sentence is sufficiently novel
            boolean gainqualifies = candidate.gainQualifies();

            // determine wether the candidate sentence contain information already seen
            // for ICTIR we used 2, for TREC 2015 we used 1
            boolean oldinfoqualifies = knownwords.getOldInfo(candidateNodeTerms, query, 2);

            if (oldinfoqualifies && gainqualifies && rankqualifies) {
                // the candidate sentence qualifies and is emitted
                emit(topicID, candidateNode, candidateNode.getContent());
                
                // the sentence is added to the pools of known words and emitted
                // sentences
                knownwords.addKownWordCombinations(candidateNodeTerms);
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
        // although an array list, this should only contain unique words
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
         * @return true when sufficiently novel, in the original version when the
         * percentage of 2-word combination in the sentence that co-occur in other
         * salient sentences in the cluster and are not in emitted sentences exceeds
         * the threshold minInformationGain. 
         */
        public boolean gainQualifies() {
            //The query terms are discounted because of their very frequent appearance.
            HashSet<String> titleminusquery = new HashSet(candidateNodeTerms);
            titleminusquery.removeAll(query.getTerms());
            
            double gain = knownwords.estimateInformationGain(candidateNodeTerms, cluster, node);
            double gainthreshold = ((titleminusquery.size() * (candidateNodeTerms.size() - 1)) * minInformationGain);
            if (watch) {
                log.info("gain %f thresh %f title %d title-q %d\n", gain, gainthreshold, candidateNodeTerms.size(), titleminusquery.size());
            }
            return titleminusquery.size() > 0 && gain >= gainthreshold;
        }
        
        @Override
        public int compareTo(Sentence o) {
            return relevance > o.relevance ? -1 : relevance < o.relevance ? 1 : 0;
        }
    }

    public void sharePool(TopKRetriever retriever) {
        this.poolEmittedSentences = retriever.poolEmittedSentences;
        this.knownwords = retriever.knownwords;
    }

    public void qualify(ArrayList<Cluster<N>> clusters) throws IOException, InterruptedException {
        ArrayList<Sentence> candidates = new ArrayList();
        for (Cluster<N> cluster : clusters) {
            N candidateNode = (N) cluster.getNodes().get(cluster.getNodes().size() - 1);
            cluster.stripCluster();
            HashSet<String> candidateNodeTermSet = new HashSet(tokenizer.tokenize(candidateNode.getContent()));
            ArrayList<String> candidateNodeTerms = new ArrayList(candidateNodeTermSet);

            boolean contains = cluster.getNodes().contains(candidateNode);
            boolean clusterqualifies = qualifies(cluster, query);
            boolean nodeQualifies = qualifyLength(candidateNode, candidateNodeTermSet);

            watch = false;
            if (watch) {
                log.info("url %d %d %s %s", candidateNode.getID(), candidateNode.getDomain(), candidateNode.getContent(), candidateNodeTermSet);
                for (Node b : cluster.getNodes()) {
                    log.info("base %d %d %s", b.getID(), b.getDomain(), b.getContent());
                }
                log.info("%d %b %b %b %s", candidateNode.getID(), contains, clusterqualifies, nodeQualifies, candidateNode.getContent());
            }
            if (contains && clusterqualifies && nodeQualifies) { // todo extend relevance model if sentence does not qualify and using restrictions
                relevanceVector.addPre(cluster, candidateNode);
                if (candidates.size() == 0) {
                    relevanceVector.removeExpired(candidateNode.getCreationTime());
                }
                candidates.add(new Sentence(candidateNode, cluster, candidateNodeTerms));
            }
        }
        if (candidates.size() > 0) {
            if (relevanceVector.hasChanged()) {
                rankEmittedSentences(poolEmittedSentences);
            }
            rankEmittedSentences(candidates);
            while (candidates.size() > 0) {
                double similarityRankR = (poolEmittedSentences.size() >= minRankObtained) ? poolEmittedSentences.get(minRankObtained - 1).relevance : 0;
                Sentence candidate = candidates.remove(0);
                boolean rankqualifies = (poolEmittedSentences.size() < minRankObtained || candidate.relevance >= similarityRankR);
                boolean gainqualifies = candidate.gainQualifies();

                boolean oldinfoqualifies = knownwords.getOldInfo(candidate.candidateNodeTerms, query, 1);

                if (watch) {
                    for (Node b : candidate.cluster.getNodes()) {
                        log.info("base %d %d %s", b.getID(), b.getDomain(), b.getContent());
                    }
                    log.info("%s", this.relevanceVector);
                    log.info("%d %d %s", candidate.cluster.getID(), candidate.node.getID(), candidate.node.getContent());
                    for (int i = 0; i < this.emittedSentences.size(); i++) {
                        log.info("%d %f %s", i + 1, emittedSentences.get(i).relevance, emittedSentences.get(i).node.getContent());
                    }
                    log.info("simtobeat %f sim %f\n", similarityRankR, candidate.relevance);
                    log.info("oldinfo %b gain %b rank %b\n", oldinfoqualifies, gainqualifies, rankqualifies);
                }
                if (oldinfoqualifies && gainqualifies && rankqualifies) {
                    emit(topicID, candidate.node, candidate.node.getContent());
                    knownwords.addKownWordCombinations(candidate.candidateNodeTerms);
                    emittedSentences.add(candidate);
                    if (poolEmittedSentences != emittedSentences) {
                        poolEmittedSentences.add(candidate);
                    }
                }
                relevanceVector.addPost(candidate.node);
            }
        }
    }

    /**
     * @param candidateSentence
     * @param candidateSentenceTerms unique non stop words in sentence
     * @return true if the candidate is without the window of the topic and the
     * length does not exceed the threshold l
     */
    public boolean qualifyLength(N candidateSentence, HashSet<String> candidateSentenceTerms) {
        return candidateSentence.getCreationTime() >= topicStartTime
                && candidateSentence.getCreationTime() <= topicEndTime
                && candidateSentenceTerms.size() <= maxSentenceLengthWords;
    }

    /**
     * @param cluster
     * @param query
     * @return true when a sentence in the cluster contains all query terms
     */
    public boolean qualifies(Cluster<N> cluster, Query query) {
        for (N url : cluster.getNodes()) {
            if (query.fullMatch(url.getTerms())) {
                return true;
            }
        }
        return false;
    }

    /**
     * re rank all emitted sentences using the relevance vector
     */
    public void rankEmittedSentences(ArrayList<? extends Sentence> list) {
        for (Sentence e : list) {
            e.relevance = e.vector.cossim(relevanceVector);
        }
        Collections.sort(list);
    }

    public static ArrayList<Long> getNearestNeigborIds(String nn) {
        ArrayList<Long> result = new ArrayList();
        for (String n : nn.split(",")) {
            result.add(Long.parseLong(n));
        }
        return result;
    }

    public static ArrayList<Double> getNearestNeighborScores(String nn) {
        ArrayList<Double> result = new ArrayList();
        for (String n : nn.split(",")) {
            result.add(Double.parseDouble(n));
        }
        return result;
    }

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

        public RelevanceVector(Collection<String> query) {
            sentenceAddedToRelevanceModel = new HashSet();
            sentenceAddedRelevanceModel = new OrderedQueueMap();
            add(query);
        }

        public void add(RelevanceVector other) {
            super.add(other);
            sentenceAddedToRelevanceModel.addAll(other.sentenceAddedToRelevanceModel);
            sentenceAddedRelevanceModel.addAll(other.sentenceAddedRelevanceModel);
        }

        public void add(Cluster<NodeSentence> c) {
            for (NodeSentence base : c.getCore()) {
                if (!sentenceAddedToRelevanceModel.contains(base.getID())) {
                    sentenceAddedToRelevanceModel.add(base.getID());
                    this.add(base.getTerms());
                    this.sentenceAddedRelevanceModel.add(base.getCreationTime(), new doc(base.getID(), base.getTerms()));
                }
            }
        }

        public void addPre(Cluster<N> c, N candidate) {
            for (N base : c.getCore()) {
                if (base != candidate && !sentenceAddedToRelevanceModel.contains(base.getID())) {
                    sentenceAddedToRelevanceModel.add(base.getID());
                    this.add(base.getTerms());
                    this.sentenceAddedRelevanceModel.add(base.getCreationTime(), new doc(base.getID(), base.getTerms()));
                }
            }
        }

        public void addPost(N candidate) {
            if (!sentenceAddedToRelevanceModel.contains(candidate.getID())) {
                sentenceAddedToRelevanceModel.add(candidate.getID());
                this.add(candidate.getTerms());
                this.sentenceAddedRelevanceModel.add(candidate.getCreationTime(), new doc(candidate.getID(), candidate.getTerms()));
            }
        }

        public void add(long timestamp, long id, Collection<String> terms) {
            this.add(terms);
            this.sentenceAddedRelevanceModel.add(timestamp, new doc(id, terms));
            this.sentenceAddedToRelevanceModel.add(id);
        }

        public void removeExpired(long timestamp) {
            timestamp -= windowRelevanceModelSeconds;
            while (sentenceAddedRelevanceModel.size() > 0) {
                Map.Entry<Long, doc> entry = sentenceAddedRelevanceModel.peek();
                if (entry.getKey() < timestamp) {
                    relevanceVector.remove(entry.getValue().content);
                    sentenceAddedRelevanceModel.poll();
                } else {
                    break;
                }
            }
        }

        public void remove(Collection<String> terms) {
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

        public boolean hasChanged() {
            return magnitude == 0;
        }
    }

    public abstract class KnownWords<N extends Node> {

        public abstract void add(KnownWords other);

//        public void addQueries(ArrayList<ArrayList<String>> queries) {
//            for (ArrayList<String> query : queries) {
//                addKownWordCombinations(query);
//            }
//        }
        public abstract void addKownWordCombinations(ArrayList<String> terms);

        public abstract void addKownWordCombinations(Query query);

        public abstract double estimateInformationGain(ArrayList<String> terms, Cluster<N> cluster, Node candidateSentence);

        public abstract boolean getOldInfo(ArrayList<String> terms, Query query, int e);
    }

    protected class KnownWordsCombinations extends KnownWords<N> {

        protected HashMapSet<String, String> knownwords = new HashMapSet();

        @Override
        public void add(KnownWords other) {
            knownwords.addAll(((KnownWordsCombinations) other).knownwords);
        }

        @Override
        public void addKownWordCombinations(Query query) {
            for (ArrayList<String> terms : query.getQueries()) {
                addKownWordCombinations(terms);
            }
        }

        @Override
        public void addKownWordCombinations(ArrayList<String> terms) {
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

        public double estimateInformationGain(ArrayList<String> terms, Cluster<N> cluster, Node candidateSentence) {
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
        public boolean getOldInfo(ArrayList<String> terms, Query query, int e) {
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

    protected class KnownWordsUnigram extends KnownWords<N> {

        public KnownWordsUnigram() {
        }

        HashSet<String> knownwords = new HashSet();

        public void add(KnownWords other) {
            knownwords.addAll(((KnownWordsUnigram) other).knownwords);
        }

        public void addKownWordCombinations(Query query) {
            for (ArrayList<String> terms : query.getQueries()) {
                addKownWordCombinations(terms);
            }
        }

        public void addKownWordCombinations(ArrayList<String> terms) {
            knownwords.addAll(terms);
        }

        public double estimateInformationGain(ArrayList<String> terms, Cluster<N> cluster, Node candidateSentence) {
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
        public boolean getOldInfo(ArrayList<String> terms, Query query, int e) {
            for (String term : terms) {
                if (knownwords.contains(term)) {
                    if (--e < 1) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
