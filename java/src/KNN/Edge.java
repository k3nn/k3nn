package KNN;

/**
 *
 * @author jeroen
 */
public class Edge<U extends Url> {

    final U url;
    final double score;

    public Edge(U a, double score) {
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
