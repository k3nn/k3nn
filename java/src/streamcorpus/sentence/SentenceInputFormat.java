package streamcorpus.sentence;

import io.github.repir.tools.hadoop.Structured.InputFormat;

public class SentenceInputFormat extends InputFormat<SentenceFile, SentenceWritable> {

    public SentenceInputFormat() {
        super(SentenceFile.class);
    }
}
