package Sentence;

import io.github.htools.hadoop.io.InputFormat;
import io.github.htools.hadoop.io.StructuredFileInputFormat;

public class SentenceInputFormat extends StructuredFileInputFormat<SentenceFile, SentenceWritable> {

    public SentenceInputFormat() {
        super(SentenceFile.class);
    }
}
