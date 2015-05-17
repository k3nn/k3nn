package KNN;

/**
 * A nearest neighbor edge to another node, with a score that reflects the similarity
 * @author jeroen
 */
public class Edge<U extends Node> {

    final U node;
    final double score;

    public Edge(U a, double score) {
        this.node = a;
        this.score = score;
    }
    
    public Node getNode() {
        return node;
    }
    
    public double getScore() {
        return score;
    }
    
    @Override
    public String toString() {
        return new StringBuilder().append("Edge(").append(node.toString()).append("[")
                .append(node.isClustered()?node.getCluster().id:-1).append("],").append(score).append(")").toString();
    }
}
