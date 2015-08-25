package Cluster;

import io.github.htools.hadoop.io.StructuredFileInputFormat;

public class ClusterInputFormat extends StructuredFileInputFormat<ClusterFile, ClusterWritable> {

    public ClusterInputFormat() {
        super(ClusterFile.class);
    }
}
