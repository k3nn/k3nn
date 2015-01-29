package RelCluster;

import io.github.repir.tools.hadoop.io.StructuredFileInputFormat;

public class RelClusterInputFormat extends StructuredFileInputFormat<RelClusterFile, RelClusterWritable> {

    public RelClusterInputFormat() {
        super(RelClusterFile.class);
    }
}
