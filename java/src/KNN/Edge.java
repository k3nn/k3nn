package KNN;

import io.github.repir.tools.Lib.Log;

/**
 *
 * @author jeroen
 */
public class Edge {

    final Url url;
    final double score;

    public Edge(Url a, double score) {
        this.url = a;
        this.score = score;
    }
    
    public Url getUrl() {
        return url;
    }
    
    public double getScore() {
        return score;
    }
    
    public String toString() {
        return new StringBuilder().append("Edge(").append(url.toString()).append("[")
                .append(url.isClustered()?url.getCluster().id:-1).append("],").append(score).append(")").toString();
    }
}
