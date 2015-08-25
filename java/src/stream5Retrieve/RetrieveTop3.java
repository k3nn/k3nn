package stream5Retrieve;

import KNN.Cluster;
import KNN.Edge;
import KNN.Stream;
import KNN.Node;
import KNN.NodeD;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import io.github.htools.collection.ArrayMap;
import io.github.htools.collection.ArrayMap3;
import io.github.htools.collection.HashMapInt;
import io.github.htools.collection.HashMapSet;
import io.github.htools.collection.OrderedQueueMap;
import io.github.htools.io.Datafile;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.lib.Log;
import io.github.htools.type.TermVectorInt;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;

public abstract class RetrieveTop3<N extends Node> {

    private static final Log log = new Log(RetrieveTop3.class);

    // Map of Similarity Scores for Nodes that were emitted
    protected ArrayList<Emission> emittedSentences;
    protected ArrayList<Emission> poolEmittedSentences;
    protected KnownWords knownwords;
    protected RelevanceVector relevanceVector;
    // tokenizes on non-alphfanumeric chars, remove stopwords, lowercase, no stemmer
    protected DefaultTokenizer tokenizer = getTokenizer();

    // TREC topic, start and end identify the interval between which to look for
    // sentences
    protected long topicStartTime;
    protected long topicEndTime;
    protected int topicID;

    protected boolean watch; // for debugging
    protected Query query;

    // requirements to qualify sentences for emission: minRankObtained=r,
    // minInformationGain=g, windowRelevanceModelHours=h, maxSentenceLengthWords=l
    public int minRankObtained = 5;
    public int maxSentenceLengthWords = 20;
    public double minInformationGain = 0.5;
    public double windowRelevanceModelHours = 1;
    protected int windowRelevanceModelSeconds;

    public void add(RetrieveTop3<N> other) {
        emittedSentences.addAll(other.emittedSentences);
        if (knownwords != other.knownwords) {
            knownwords.add(other.knownwords);
        }
        relevanceVector.add(other.relevanceVector);
    }

    public abstract void emit(int topic, N u, String title) throws IOException, InterruptedException;

    /**
     * Initialize streaming sentence clusters and qualifying sentences
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
        windowRelevanceModelSeconds = (int) (windowRelevanceModelHours * 60 * 60);
        emittedSentences = new ArrayList();
        poolEmittedSentences = emittedSentences;
        knownwords = createKnownWords();
        this.query = query;
        relevanceVector = new RelevanceVector(this.query.getTerms());
        knownwords.addKownWordCombinations(this.query);
    }

    protected KnownWords createKnownWords() {
        return new KnownWordsBigram();
    }

    public DefaultTokenizer getTokenizer() {
        return Stream.getUnstemmedTokenizer();
    }

    public void stream(Cluster<N> cluster) throws IOException, InterruptedException {
        // construct a cluster object from the streamclusterwritable
        //Cluster<N> c = createCluster(writable);
        // the last sentence is the only sentence that can qualify for emission
        N candidateNode = (N) cluster.getNodes().get(cluster.getNodes().size() - 1);
        // for the BaseCluster variant, strip nodes that are not connected back to
        // returned result is a shallow clone of the original cluster
        cluster = cluster.stripCluster();

        // tokenize sentence to list of unique non stop words
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
            relevanceVector.removeExpired(candidateNode.getCreationTime());
            Candidate candidate = new Candidate(candidateNode, cluster, candidateNodeTerms);
            rankEmittedSentences(poolEmittedSentences);
            candidate.relevance = candidate.vector.cossim(relevanceVector);
            double similarityRankR = (poolEmittedSentences.size() >= minRankObtained) ? poolEmittedSentences.get(minRankObtained - 1).relevance : 0;
            boolean rankqualifies = (poolEmittedSentences.size() < minRankObtained || candidate.relevance >= similarityRankR);

            boolean gainqualifies = candidate.gainQualifies();

            boolean oldinfoqualifies = knownwords.getOldInfo(candidateNodeTerms, query, 1);

            if (watch) {
                for (Node b : cluster.getNodes()) {
                    log.info("base %d %d %s", b.getID(), b.getDomain(), b.getContent());
                }
                log.info("%d %b %b %b %s", candidateNode.getID(), contains, clusterqualifies, nodeQualifies, candidateNode.getContent());
                log.info("%s", this.relevanceVector);
                log.info("%d %d %s", cluster.getID(), candidateNode.getID(), candidateNode.getContent());
                for (int i = 0; i < this.poolEmittedSentences.size(); i++) {
                    log.info("%d %f %s", i + 1, poolEmittedSentences.get(i), poolEmittedSentences.get(i).node.getContent());
                }
                log.info("simtobeat %f sim %f\n", similarityRankR, candidate.relevance);
                log.info("oldinfo %b gain %b rank %b\n", oldinfoqualifies, gainqualifies, rankqualifies);
            }
            if (oldinfoqualifies && gainqualifies && rankqualifies) {
                emit(topicID, candidateNode, candidateNode.getContent());
                knownwords.addKownWordCombinations(candidateNodeTerms);
                Emission e = candidate.getEmission();
                emittedSentences.add(e);
                if (emittedSentences != poolEmittedSentences) {
                    poolEmittedSentences.add(e);
                }
            }
            relevanceVector.addPost(candidateNode);
        }
    }

    public class Candidate extends Emission {

        public ArrayList<String> candidateNodeTerms;
        public Cluster<N> cluster;

        public Candidate(N node, Cluster<N> cluster, ArrayList<String> terms) {
            super(node, new TermVectorInt(terms));
            this.node = node;
            candidateNodeTerms = terms;
            this.cluster = cluster;
        }

        public Emission getEmission() {
            return new Emission(node, vector);
        }

        public boolean gainQualifies() {
            HashSet<String> titleminusquery = new HashSet(candidateNodeTerms);
            titleminusquery.removeAll(query.getTerms());
            double gain = knownwords.estimateInformationGain(candidateNodeTerms, cluster, node);
            double gainthreshold = ((titleminusquery.size() * (candidateNodeTerms.size() - 1)) * minInformationGain);
            //gainthreshold = (((candidateNodeTerms.size() - queryterms.size()) * (candidateNodeTerms.size() - 1)) * minInformationGain);
            if (watch) {
                log.info("gain %f thresh %f title %d title-q %d\n", gain, gainthreshold, candidateNodeTerms.size(), titleminusquery.size());
            }
            return titleminusquery.size() > 0 && gain >= gainthreshold;
        }
    }

    public class Emission implements Comparable<Emission> {

        public N node;
        public double relevance;
        public TermVectorInt vector;

        public Emission(N node, TermVectorInt vector) {
            this.node = node;
            this.vector = vector;
        }

        @Override
        public int compareTo(Emission o) {
            return relevance > o.relevance ? -1 : relevance < o.relevance ? 1 : 0;
        }
    }

    public void sharePool(RetrieveTop3 retriever) {
        this.poolEmittedSentences = retriever.poolEmittedSentences;
        this.knownwords = retriever.knownwords;
    }

    public void stream(ArrayList<Cluster<N>> clusters) throws IOException, InterruptedException {
        ArrayList<Candidate> candidates = new ArrayList();
        for (Cluster<N> cluster : clusters) {
            N candidateNode = (N) cluster.getNodes().get(cluster.getNodes().size() - 1);
            cluster = cluster.stripCluster();
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
                candidates.add(new Candidate(candidateNode, cluster, candidateNodeTerms));
            }
        }
        if (candidates.size() > 0) {
            if (relevanceVector.hasChanged()) {
                rankEmittedSentences(poolEmittedSentences);
            }
            rankEmittedSentences(candidates);
            while (candidates.size() > 0) {
                double similarityRankR = (poolEmittedSentences.size() >= minRankObtained) ? poolEmittedSentences.get(minRankObtained - 1).relevance : 0;
                Candidate candidate = candidates.remove(0);
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
                    Emission e = candidate.getEmission();
                    emittedSentences.add(e);
                    if (poolEmittedSentences != emittedSentences) {
                        poolEmittedSentences.add(e);
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
    public void rankEmittedSentences(ArrayList<? extends Emission> list) {
        for (Emission e : list) {
            e.relevance = e.vector.cossim(relevanceVector);
        }
        Collections.sort(list);
    }

    /**
     * @param clusterWritable
     * @return reconstructed nearest neighbor cluster from stored record
     */
    public Cluster<NodeD> createCluster(ClusterWritable clusterWritable) {
        Stream<NodeD> s = new Stream();
        for (NodeWritable r : clusterWritable.nodes) {
            HashSet<String> title = new HashSet(tokenizer.tokenize(r.content));
            NodeD u = new NodeD(r.sentenceID, r.domain, r.content, title, r.creationtime, r.getUUID(), r.sentenceNumber);
            s.nodes.put(u.getID(), u);
        }
        Cluster<NodeD> c = s.createCluster(clusterWritable.clusterid);
        for (NodeWritable r : clusterWritable.nodes) {
            Node url = s.nodes.get(r.sentenceID);
            ArrayList<Long> nn = getNN(r.nnid);
            ArrayList<Double> score = getScores(r.nnscore);
            for (int i = 0; i < nn.size(); i++) {
                Node u = s.nodes.get(nn.get(i));
                Edge e = new Edge(u, score.get(i));
                url.add(e);
            }
            url.setCluster(c);
        }
        c.setBase(Cluster.getBase(c.getNodes()));
        return c;
    }

    public static ArrayList<Long> getNN(String nn) {
        ArrayList<Long> result = new ArrayList();
        for (String n : nn.split(",")) {
            result.add(Long.parseLong(n));
        }
        return result;
    }

    public static ArrayList<Double> getScores(String nn) {
        ArrayList<Double> result = new ArrayList();
        for (String n : nn.split(",")) {
            result.add(Double.parseDouble(n));
        }
        return result;
    }

    public static HashMap<Integer, TopicWritable> getTopics(String topicfile) {
        Datafile df = new Datafile(topicfile);
        TopicFile tf = new TopicFile(df);
        return tf.getMap();
    }

    public ArrayMap3<Long, Long, HashSet<String>> getRelevanceEntries() {
        ArrayMap3<Long, Long, HashSet<String>> entries = new ArrayMap3();
        for (Map.Entry<Long, RelevanceVector.doc> entry : relevanceVector.sentenceAddedRelevanceModel) {
            entries.add(entry.getKey(), entry.getValue().sentenceid, entry.getValue().content);
        }
        return entries;
    }

    protected class RelevanceVector extends TermVectorInt {

        class doc {

            long sentenceid;
            HashSet<String> content;

            doc(long id, HashSet<String> content) {
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

        public void add(Cluster<NodeD> c) {
            for (NodeD base : c.getBase()) {
                if (!sentenceAddedToRelevanceModel.contains(base.getID())) {
                    sentenceAddedToRelevanceModel.add(base.getID());
                    this.add(base.getTerms());
                    this.sentenceAddedRelevanceModel.add(base.getCreationTime(), new doc(base.getID(), base.getTerms()));
                }
            }
        }

        public void addPre(Cluster<N> c, N candidate) {
            for (N base : c.getBase()) {
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

        public void add(long timestamp, long id, HashSet<String> terms) {
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

    protected class KnownWordsBigram extends KnownWords<N> {

        protected HashMapSet<String, String> knownwords = new HashMapSet();

        @Override
        public void add(KnownWords other) {
            knownwords.addAll(((KnownWordsBigram) other).knownwords);
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
