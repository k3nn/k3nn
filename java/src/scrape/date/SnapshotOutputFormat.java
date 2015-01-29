package scrape.date;

import io.github.repir.tools.hadoop.io.OutputFormat;
import io.github.repir.tools.hadoop.Job;
/**
 *
 * @author jeroen
 */
public class SnapshotOutputFormat extends OutputFormat<SnapshotFile, SnapshotWritable> {

    public SnapshotOutputFormat() {
        super(SnapshotFile.class, SnapshotWritable.class);
    }

    public SnapshotOutputFormat(Job job) {
        super(job, SnapshotFile.class, SnapshotWritable.class);
    }

}
