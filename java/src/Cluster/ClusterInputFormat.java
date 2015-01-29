package Cluster;

import io.github.repir.tools.hadoop.io.StructuredFileInputFormat;

public class ClusterInputFormat extends StructuredFileInputFormat<ClusterFile, ClusterWritable> {

    public ClusterInputFormat() {
        super(ClusterFile.class);
    }
}
