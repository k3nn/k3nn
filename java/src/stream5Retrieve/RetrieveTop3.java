package stream5Retrieve;

import KNN.Cluster;
import KNN.Edge;
import KNN.Stream;
import KNN.Node;
import KNN.NodeD;
import Cluster.ClusterWritable;
import Cluster.NodeWritable;
import io.github.repir.tools.collection.ArrayMap;
import io.github.repir.tools.collection.ArrayMap3;
import io.github.repir.tools.collection.HashMapInt;
import io.github.repir.tools.collection.HashMapSet;
import io.github.repir.tools.collection.OrderedQueueMap;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.extract.DefaultTokenizer;
import io.github.repir.tools.lib.Log;
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
    protected ArrayMap<Double, N> emittedSentences;
    protected ArrayMap<Double, N> poolEmittedSentences;
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
    protected HashSet<String> queryterms;

    // requirements to qualify sentences for emission: minRankObtained=r,
    // minInformationGain=g, windowRelevanceModelHours=h, maxSentenceLengthWords=l
    public int minRankObtained = 5;
    public int maxSentenceLengthWords = 20;
    public double minInformationGain = 0.5;
    public double windowRelevanceModelHours = 1;
    protected int windowRelevanceModelSeconds;

    public void add(RetrieveTop3<N> other) {
        for (Map.Entry<Double, N> entry : other.emittedSentences) {
            emittedSentences.add(entry);
        }
        if (knownwords != other.knownwords)
            knownwords.add(other.knownwords);
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
    public void init(int topicid, long topicstart, long topicend, String query) throws IOException {
        this.topicID = topicid;
        this.topicStartTime = topicstart;
        this.topicEndTime = topicend;
        windowRelevanceModelSeconds = (int) (windowRelevanceModelHours * 60 * 60);
        emittedSentences = new ArrayMap();
        poolEmittedSentences = emittedSentences;
        knownwords = createKnownWords();
        queryterms = new HashSet(tokenizer.tokenize(query));
        relevanceVector = new RelevanceVector(queryterms);
        knownwords.addKownWordCombinations(new ArrayList(queryterms));
    }

    protected KnownWords createKnownWords() {
        return new KnownWordsBigram();
    }
    
    public DefaultTokenizer getTokenizer() {
        return Stream.getUnstemmedTokenizer();
    }

    public void stream(Cluster<N> c) throws IOException, InterruptedException {
        // construct a cluster object from the streamclusterwritable
        //Cluster<N> c = createCluster(writable);
        // the last sentence is the only sentence that can qualify for emission
        N candidateNode = (N) c.getNodes().get(c.getNodes().size() - 1);
        // for the BaseCluster variant, strip nodes that are not connected back to
        // returned result is a shallow clone of the original cluster
        c = c.stripCluster();

        // tokenize sentence to list of unique non stop words
        HashSet<String> candidateNodeTermSet = new HashSet(tokenizer.tokenize(candidateNode.getContent()));
        ArrayList<String> candidateNodeTerms = new ArrayList(candidateNodeTermSet);

        boolean contains = c.getNodes().contains(candidateNode);
        boolean clusterqualifies = qualifies(c, queryterms);
        boolean nodeQualifies = qualifyLength(candidateNode, candidateNodeTermSet);

        watch = false;
        if (watch) {
            log.info("url %d %d %s %s", candidateNode.getID(), candidateNode.getDomain(), candidateNode.getContent(), candidateNodeTermSet);
            for (Node b : c.getNodes()) {
                log.info("base %d %d %s", b.getID(), b.getDomain(), b.getContent());
            }
            log.info("%d %b %b %b %s", candidateNode.getID(), contains, clusterqualifies, nodeQualifies, candidateNode.getContent());
        }
        if (contains && clusterqualifies && nodeQualifies) { // todo extend relevance model if sentence does not qualify and using restrictions
            relevanceVector.addPre(c, candidateNode);
            relevanceVector.removeExpired(candidateNode.getCreationTime());
            rankEmittedSentences();
            double similarityCandidateNode = relevanceVector.cosSim(candidateNodeTerms);
            double similarityRankR = (emittedSentences.size() >= minRankObtained) ? emittedSentences.getKey(minRankObtained - 1) : 0;
            boolean rankqualifies = (emittedSentences.size() < minRankObtained || similarityCandidateNode >= similarityRankR);

            HashSet<String> titleminusquery = new HashSet(candidateNodeTerms);
            titleminusquery.removeAll(queryterms);
            double gain = knownwords.estimateInformationGain(candidateNodeTerms, c, candidateNode);
            double gainthreshold = ((titleminusquery.size() * (candidateNodeTerms.size() - 1)) * minInformationGain);
            //gainthreshold = (((candidateNodeTerms.size() - queryterms.size()) * (candidateNodeTerms.size() - 1)) * minInformationGain);
            boolean gainqualifies = gain >= gainthreshold;

            boolean oldinfoqualifies = knownwords.getOldInfo(candidateNodeTerms, queryterms) > 0;

            if (watch) {
                for (Node b : c.getNodes()) {
                    log.info("base %d %d %s", b.getID(), b.getDomain(), b.getContent());
                }
                log.info("%d %b %b %b %s", candidateNode.getID(), contains, clusterqualifies, nodeQualifies, candidateNode.getContent());
                log.info("%s", this.relevanceVector);
                log.info("%d %d %s", c.getID(), candidateNode.getID(), candidateNode.getContent());
                for (int i = 0; i < this.emittedSentences.size(); i++) {
                    log.info("%d %f %s", i + 1, emittedSentences.getKey(i), emittedSentences.getValue(i).getContent());
                }
                log.info("gain %f thresh %f title %d title-q %d\n", gain, gainthreshold, candidateNodeTerms.size(), titleminusquery.size());
                log.info("simtobeat %f sim %f\n", similarityRankR, similarityCandidateNode);
                log.info("oldinfo %b gain %b rank %b\n", oldinfoqualifies, gainqualifies, rankqualifies);
            }
            if (oldinfoqualifies && gainqualifies && rankqualifies) {
                emit(topicID, candidateNode, candidateNode.getContent());
                knownwords.addKownWordCombinations(candidateNodeTerms);
                emittedSentences.add(similarityCandidateNode, candidateNode);
            }
            relevanceVector.addPost(candidateNode);
        }
    }

    class Candidate implements Comparable<Candidate> {

        N node;
        double similarityCandidateNode;
        ArrayList<String> candidateNodeTerms;
        Cluster<N> cluster;

        public Candidate(N node, Cluster<N> cluster, ArrayList<String> terms) {
            this.node = node;
            this.candidateNodeTerms = terms;
            this.cluster = cluster;
        }

        @Override
        public int compareTo(Candidate o) {
            return similarityCandidateNode > o.similarityCandidateNode ? -1 : similarityCandidateNode < o.similarityCandidateNode ? 1 : 0;
        }
    }

    public void sharePool(RetrieveTop3 retriever) {
        this.poolEmittedSentences = retriever.poolEmittedSentences;
        this.knownwords = retriever.knownwords;
    }
    
    public void stream(ArrayList<Cluster<N>> clusters) throws IOException, InterruptedException {
        ArrayList<Candidate> candidates = new ArrayList();
        for (Cluster<N> c : clusters) {
            N candidateNode = (N) c.getNodes().get(c.getNodes().size() - 1);
            c = c.stripCluster();
            HashSet<String> candidateNodeTermSet = new HashSet(tokenizer.tokenize(candidateNode.getContent()));
            ArrayList<String> candidateNodeTerms = new ArrayList(candidateNodeTermSet);

            boolean contains = c.getNodes().contains(candidateNode);
            boolean clusterqualifies = qualifies(c, queryterms);
            boolean nodeQualifies = qualifyLength(candidateNode, candidateNodeTermSet);

            watch = false;
            if (watch) {
                log.info("url %d %d %s %s", candidateNode.getID(), candidateNode.getDomain(), candidateNode.getContent(), candidateNodeTermSet);
                for (Node b : c.getNodes()) {
                    log.info("base %d %d %s", b.getID(), b.getDomain(), b.getContent());
                }
                log.info("%d %b %b %b %s", candidateNode.getID(), contains, clusterqualifies, nodeQualifies, candidateNode.getContent());
            }
            if (contains && clusterqualifies && nodeQualifies) { // todo extend relevance model if sentence does not qualify and using restrictions
                relevanceVector.addPre(c, candidateNode);
                if (candidates.size() == 0) {
                    relevanceVector.removeExpired(candidateNode.getCreationTime());
                }
                candidates.add(new Candidate(candidateNode, c, candidateNodeTerms));
            }
        }
        if (candidates.size() > 0) {
            for (Candidate c : candidates) {
                c.similarityCandidateNode = relevanceVector.cosSim(c.candidateNodeTerms);
            }
            Collections.sort(candidates);
            rankEmittedSentences();
            double similarityRankR = (emittedSentences.size() >= minRankObtained) ? emittedSentences.getKey(minRankObtained - 1) : 0;
            while (candidates.size() > 0) {
                Candidate candidate = candidates.remove(0);
                boolean rankqualifies = (emittedSentences.size() < minRankObtained || candidate.similarityCandidateNode >= similarityRankR);
                HashSet<String> titleminusquery = new HashSet(candidate.candidateNodeTerms);
                titleminusquery.removeAll(queryterms);
                double gain = knownwords.estimateInformationGain(candidate.candidateNodeTerms, candidate.cluster, candidate.node);
                double gainthreshold = ((titleminusquery.size() * (candidate.candidateNodeTerms.size() - 1)) * minInformationGain);
                //gainthreshold = (((candidateNodeTerms.size() - queryterms.size()) * (candidateNodeTerms.size() - 1)) * minInformationGain);
                boolean gainqualifies = gain >= gainthreshold;

                boolean oldinfoqualifies = knownwords.getOldInfo(candidate.candidateNodeTerms, queryterms) > 0;

                if (watch) {
                    for (Node b : candidate.cluster.getNodes()) {
                        log.info("base %d %d %s", b.getID(), b.getDomain(), b.getContent());
                    }
                    log.info("%s", this.relevanceVector);
                    log.info("%d %d %s", candidate.cluster.getID(), candidate.node.getID(), candidate.node.getContent());
                    for (int i = 0; i < this.emittedSentences.size(); i++) {
                        log.info("%d %f %s", i + 1, emittedSentences.getKey(i), emittedSentences.getValue(i).getContent());
                    }
                    log.info("gain %f thresh %f title %d title-q %d\n", gain, gainthreshold, candidate.candidateNodeTerms.size(), titleminusquery.size());
                    log.info("simtobeat %f sim %f\n", similarityRankR, candidate.similarityCandidateNode);
                    log.info("oldinfo %b gain %b rank %b\n", oldinfoqualifies, gainqualifies, rankqualifies);
                }
                if (oldinfoqualifies && gainqualifies && rankqualifies) {
                    emit(topicID, candidate.node, candidate.node.getContent());
                    knownwords.addKownWordCombinations(candidate.candidateNodeTerms);
                    emittedSentences.add(candidate.similarityCandidateNode, candidate.node);
                    if (poolEmittedSentences != emittedSentences)
                        poolEmittedSentences.add(candidate.similarityCandidateNode, candidate.node);
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
     * @param node
     * @param query
     * @return true if node contains all query terms
     */
    public boolean containsEntireQuery(Node node, HashSet<String> query) {
        for (String term : query) {
            if (!node.getTerms().contains(term)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @param cluster
     * @param query
     * @return true when a sentence in the cluster contains all query terms
     */
    public boolean qualifies(Cluster<N> cluster, HashSet<String> query) {
        for (N url : cluster.getNodes()) {
            if (containsEntireQuery(url, query)) {
                return true;
            }
        }
        return false;
    }

    /**
     * re rank all emitted sentences using the relevance vector
     */
    public void rankEmittedSentences() {
        for (int i = 0; i < poolEmittedSentences.size(); i++) {
            Map.Entry<Double, N> entry = poolEmittedSentences.remove(i);
            double d = relevanceVector.cosSim(entry.getValue().getTerms());
            Map.Entry<Double, N> createEntry = ArrayMap.createEntry(d, entry.getValue());
            poolEmittedSentences.add(i, createEntry);
        }
        poolEmittedSentences.descending();
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

    protected class RelevanceVector extends HashMapInt<String> {

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

        double termsseen = 0;
        double magnitude = 0;

        public RelevanceVector(Collection<String> query) {
            sentenceAddedToRelevanceModel = new HashSet();
            sentenceAddedRelevanceModel = new OrderedQueueMap();
            add(query);
        }

        public void add(RelevanceVector other) {
            for (Map.Entry<String, Integer> entry : other.entrySet()) {
                add(entry.getKey(), entry.getValue());
            }
            sentenceAddedToRelevanceModel.addAll(other.sentenceAddedToRelevanceModel);
            sentenceAddedRelevanceModel.addAll(other.sentenceAddedRelevanceModel);
            termsseen += other.termsseen;
            magnitude = 0;
        }

        private void add(Collection<String> terms) {
            for (String term : terms) {
                add(term, 1);
            }
            termsseen += terms.size();
            magnitude = 0;
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
            termsseen -= terms.size();
            magnitude = 0;
        }

        public boolean hasChanged() {
            return magnitude == 0;
        }

        public double magnitude() {
            if (magnitude == 0 && termsseen > 0) {
                for (int i : values()) {
                    magnitude += i * i;
                }
                magnitude = Math.sqrt(magnitude);
            }
            return magnitude;
        }

        public double cosSim(Collection<String> titletokens) {
            double count = 0;
            double magnitude = 0;
            for (String term : titletokens) {
                Integer freq = get(term);
                if (freq != null) {
                    count += freq;
                    magnitude += freq * freq;
                }
            }
            if (magnitude == 0) {
                return 0;
            }
            double modelmagnitude = magnitude();
            if (modelmagnitude == 0) {
                return 0;
            }
            //log.info("cossim %f %f %f", count, magnitude, magnitude());
            return count / (Math.sqrt(magnitude) * modelmagnitude);
        }
    }

    public interface KnownWords<N extends Node> {

        public void add(KnownWords other);

        public void addKownWordCombinations(ArrayList<String> terms);

        public double estimateInformationGain(ArrayList<String> terms, Cluster<N> cluster, Node candidateSentence);

        public int getOldInfo(ArrayList<String> terms, HashSet<String> query);
    }

    protected class KnownWordsBigram implements KnownWords<N> {

        protected HashMapSet<String, String> knownwords = new HashMapSet();

        public void add(KnownWords other) {
            knownwords.addAll(((KnownWordsBigram) other).knownwords);
        }

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
        public int getOldInfo(ArrayList<String> terms, HashSet<String> query) {
            int e = 0;
            if (query.size() == 1 && terms.containsAll(query)) {
                e++;
            }
            for (int i = 0; i < terms.size(); i++) {
                for (int j = i + 1; j < terms.size(); j++) {
                    HashSet<String> list = knownwords.get(terms.get(i));
                    if (list != null && list.contains(terms.get(j)) && (query.contains(terms.get(i)) || query.contains(terms.get(j)))) {
                        e++;
                    } else if (query.contains(terms.get(i)) && query.contains(terms.get(j))) {
                        e++; // necessary?
                    }
                }
            }
            return e;
        }

    }

    protected class KnownWordsUnigram implements KnownWords<N> {

        public KnownWordsUnigram() {
        }

        HashSet<String> knownwords = new HashSet();

        public void add(KnownWords other) {
            knownwords.addAll(((KnownWordsUnigram) other).knownwords);
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
        public int getOldInfo(ArrayList<String> terms, HashSet<String> query) {
            int e = 0;
            for (String term : terms) {
                if (knownwords.contains(term)) {
                    e++;
                }
            }
            return e;
        }
    }
}
