package kbaeval;

import io.github.repir.tools.hadoop.io.StructuredFileInputFormat;

public class TopicInputFormat extends StructuredFileInputFormat<TopicFile, TopicWritable> {

    public TopicInputFormat() {
        super(TopicFile.class);
    }
}
