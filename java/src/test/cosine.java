package test;

import KNN.IINodesIDF;
import KNN.NodeMagnitude;
import io.github.htools.hadoop.Conf;
import io.github.htools.lib.Log;
import java.util.HashSet;
import stream4ClusterSentencesPurge.IDF.ClusterSentencesReducer.LocalStream;
/**
 *
 * @author jeroen
 */
public class cosine {
   public static final Log log = new Log( cosine.class );
   static String doc1[] = {"albert", "einstein", "relativity", "theory", "author"};
   static String doc2[] = {"albert", "einstein", "physics"};
   
   public static HashSet<String> getTerms(String[] doc) {
       HashSet<String> terms = new HashSet();
       for (String term : doc)
           terms.add(term);
       return terms;
   }
   
    public static void main(String[] args) {
        Conf conf = new Conf(args, "idf");
        LocalStream stream = new LocalStream(conf, conf.get("idf"));
        NodeMagnitude n1 = new NodeMagnitude(stream, 0, 0, "", getTerms(doc1), 0, null, 0);
        NodeMagnitude n2 = new NodeMagnitude(stream, 1, 1, "", getTerms(doc2), 0, null, 0);
        log.info("n1 magnitude %f", n1.magnitude);
        log.info("n2 magnitude %f", n2.magnitude);
        double cos = 0;
        for (String term : n2.terms) {
            if (n1.terms.contains(term)) {
                cos += ((IINodesIDF)stream.iinodes).getIDF2(term);
            }
        }
        cos /= (n1.magnitude * n2.magnitude);
        log.info("%f", cos);
    }
}
