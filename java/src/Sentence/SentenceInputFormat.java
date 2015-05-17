package Sentence;

import io.github.repir.tools.hadoop.io.InputFormat;
import io.github.repir.tools.hadoop.io.StructuredFileInputFormat;

public class SentenceInputFormat extends StructuredFileInputFormat<SentenceFile, SentenceWritable> {

    public SentenceInputFormat() {
        super(SentenceFile.class);
    }
}
