package Sentence;

import io.github.htools.hadoop.Job;
import io.github.htools.hadoop.io.OutputFormat;
/**
 *
 * @author jeroen
 */
public class SentenceOutputFormat extends OutputFormat<SentenceFile, SentenceWritable> {

    public SentenceOutputFormat() {
        super(SentenceFile.class, SentenceWritable.class);
    }

    public SentenceOutputFormat(Job job) {
        super(job, SentenceFile.class, SentenceWritable.class);
    }

}
