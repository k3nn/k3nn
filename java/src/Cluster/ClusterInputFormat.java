package Cluster;

import io.github.repir.tools.hadoop.Structured.InputFormat;

public class ClusterInputFormat extends InputFormat<ClusterFile, ClusterWritable> {

    public ClusterInputFormat() {
        super(ClusterFile.class);
    }
}
