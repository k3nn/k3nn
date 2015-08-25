package ClusterNode;

import io.github.htools.hadoop.io.StructuredFileInputFormat;

public class ClusterNodeInputFormat extends StructuredFileInputFormat<ClusterNodeFile, ClusterNodeWritable> {

    public ClusterNodeInputFormat() {
        super(ClusterNodeFile.class);
    }
}
