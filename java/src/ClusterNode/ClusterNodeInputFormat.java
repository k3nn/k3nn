package ClusterNode;

import io.github.repir.tools.hadoop.io.StructuredFileInputFormat;

public class ClusterNodeInputFormat extends StructuredFileInputFormat<ClusterNodeFile, ClusterNodeWritable> {

    public ClusterNodeInputFormat() {
        super(ClusterNodeFile.class);
    }
}
