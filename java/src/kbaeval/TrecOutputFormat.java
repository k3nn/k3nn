package kbaeval;

import io.github.repir.tools.hadoop.io.OutputFormat;
import io.github.repir.tools.hadoop.Job;
/**
 *
 * @author jeroen
 */
public class TrecOutputFormat extends OutputFormat<TrecFile, TrecWritable> {

    public TrecOutputFormat() {
        super(TrecFile.class, TrecWritable.class);
    }

    public TrecOutputFormat(Job job) {
        super(job, TrecFile.class, TrecWritable.class);
    }

}
