package RelCluster;

import io.github.repir.tools.hadoop.io.OutputFormat;
/**
 *
 * @author jeroen
 */
public class RelClusterOutputFormat extends OutputFormat<RelClusterFile, RelClusterWritable> {

    public RelClusterOutputFormat() {
        super(RelClusterFile.class, RelClusterWritable.class);
    }

}
