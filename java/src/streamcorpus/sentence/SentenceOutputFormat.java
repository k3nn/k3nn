package streamcorpus.sentence;

import io.github.repir.tools.hadoop.Job;
import io.github.repir.tools.hadoop.io.OutputFormat;
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
