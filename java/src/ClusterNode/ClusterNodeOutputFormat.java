package ClusterNode;

import io.github.repir.tools.hadoop.io.OutputFormat;
/**
 *
 * @author jeroen
 */
public class ClusterNodeOutputFormat extends OutputFormat<ClusterNodeFile, ClusterNodeWritable> {

    public ClusterNodeOutputFormat() {
        super(ClusterNodeFile.class, ClusterNodeWritable.class);
    }

}
