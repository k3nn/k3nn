package io.github.k3nn;

/**
 * A nearest neighbor edge to another node, with a score that reflects the similarity
 * @author jeroen
 */
public class Edge<N extends Node> {

    final N node;
    final double score;

    public Edge(N a, double score) {
        this.node = a;
        this.score = score;
    }
    
    public N getNode() {
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
