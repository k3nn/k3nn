package kbaeval;

import io.github.htools.hadoop.io.StructuredFileInputFormat;

public class TopicInputFormat extends StructuredFileInputFormat<TopicFile, TopicWritable> {

    public TopicInputFormat() {
        super(TopicFile.class);
    }
}
