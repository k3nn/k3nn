package StreamCluster;

import io.github.repir.tools.hadoop.io.StructuredFileInputFormat;

public class StreamClusterInputFormat extends StructuredFileInputFormat<StreamClusterFile, StreamClusterWritable> {

    public StreamClusterInputFormat() {
        super(StreamClusterFile.class);
    }
}
