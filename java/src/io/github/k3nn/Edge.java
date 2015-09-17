package io.github.k3nn;

/**
 * A nearest neighbor edge to another node, with a score that reflects the
 * similarity. Note that an edge may contain a NULL pointer to the node, when
 * the nearest neighbor has expired and both nodes are not clustered, however
 * the score may be used to decide if the nearest neighbor edges should be
 * updated regardless.
 *
 * @author jeroen
 */
public class Edge<N extends Node> {

    final N node;
    final double score;

    public Edge(N a, double score) {
        this.node = a;
        this.score = score;
    }

    /**
     * @return the nearest neighbor node, can be NULL when both nodes are not
     * clustered and the node pointed to has expired (not being able to affect
     * clustering).
     */
    public N getNode() {
        return node;
    }

    /**
     * @return Similarity score to the nearest neighbor
     */
    public double getScore() {
        return score;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("Edge(").append(node.toString()).append("[")
                .append(node.isClustered() ? node.getCluster().id : -1).append("],").append(score).append(")").toString();
    }
}
