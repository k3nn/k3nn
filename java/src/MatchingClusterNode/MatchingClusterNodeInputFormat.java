package MatchingClusterNode;

import io.github.repir.tools.hadoop.io.StructuredFileInputFormat;

public class MatchingClusterNodeInputFormat extends StructuredFileInputFormat<MatchingClusterNodeFile, MatchingClusterNodeWritable> {

    public MatchingClusterNodeInputFormat() {
        super(MatchingClusterNodeFile.class);
    }
}
