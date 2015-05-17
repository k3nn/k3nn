package KNN;

import io.github.repir.tools.collection.HashMapDouble;
import io.github.repir.tools.lib.Log;
import java.util.HashSet;
import java.util.Map;

/**
 * similarity function that scores the similarity of nodes based on cosine
 * similarity between their binary term vectors discounted by the linear
 * distance over publication time.
 * @author jeroen
 */
public enum Score {;
   public static final Log log = new Log( Score.class ); 
   public static double days3 = 60 * 60 * 24 * 3;
   
   public static double compute(Node a, Node b) {
       double t = timeliness(a.getCreationTime(), b.getCreationTime());
       return (t > 0)?t * cossim(a.getTerms(), b.getTerms()):0;
   }
   
   /**
    * @param a
    * @param b 
    * @param count number of shared terms
    * @return similarity score
    */
   public static double compute2(Node a, Node b, int count) {
       double t = timeliness(a.getCreationTime(), b.getCreationTime());
       return (t > 0)?t * count / (Math.sqrt(a.getTerms().size()) * Math.sqrt(b.getTerms().size())):0;
   }
   
   /**
    * @param time1
    * @param time2
    * @return linear discount function for the difference between two publication
    * times.
    */
   public static double timeliness(long time1, long time2) {
       double diff = time1 > time2?(time1 - time2):(time2 - time1);
       return (diff >= days3)?0.0:1.0 - (diff / days3);
   }
   
   public static double cossim(HashSet<String> map1, HashSet<String> map2) {
           if (map2.size() < map1.size())
               return cossim(map2,map1);
        double dotproduct = 0;

        for (String term : map1) {
            if (map2.contains(term))
                dotproduct++;
        }
        return dotproduct / (Math.sqrt(map1.size()) * Math.sqrt(map2.size()));
    }

   public static double cossimMLE(HashSet<String> map1, HashSet<String> map2) {
        double dotproduct = 0;
        for (String term : map1) {
            if (map2.contains(term))
                dotproduct++;
        }
        return dotproduct / (Math.sqrt(map1.size()) * Math.sqrt(dotproduct));
    }

   public static double cossim(HashMapDouble<String> map1, HashMapDouble<String> map2) {
        double dotproduct = 0;
        for (Map.Entry<String, Double> entry : map1.entrySet()) {
            Double pmap2 = map2.get(entry.getKey());
            if (pmap2 != null)
                dotproduct += pmap2 * entry.getValue();
        }
        return dotproduct / (magnitude(map1) * magnitude(map2));
    }
   
   public static double magnitude(HashMapDouble<String> map) {
       double sum = 0;
       for (Double p : map.values())
           sum += p * p;
       return Math.sqrt(sum);
   }
}
