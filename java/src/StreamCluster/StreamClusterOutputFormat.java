package StreamCluster;

import Cluster.*;
import io.github.repir.tools.hadoop.io.OutputFormat;
/**
 *
 * @author jeroen
 */
public class StreamClusterOutputFormat extends OutputFormat<StreamClusterFile, StreamClusterWritable> {

    public StreamClusterOutputFormat() {
        super(StreamClusterFile.class, StreamClusterWritable.class);
    }

}
