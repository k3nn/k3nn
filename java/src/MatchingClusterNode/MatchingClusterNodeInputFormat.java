package MatchingClusterNode;

import io.github.htools.hadoop.io.StructuredFileInputFormat;

public class MatchingClusterNodeInputFormat extends StructuredFileInputFormat<MatchingClusterNodeFile, MatchingClusterNodeWritable> {

    public MatchingClusterNodeInputFormat() {
        super(MatchingClusterNodeFile.class);
    }
}
