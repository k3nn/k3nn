package scrape.date;

import scrape.article.*;
import io.github.repir.tools.hadoop.IO.OutputFormatFolder;
import io.github.repir.tools.hadoop.Job;
/**
 *
 * @author jeroen
 */
public class SnapshotOutputFormat extends OutputFormatFolder<SnapshotFile, SnapshotWritable> {

    public SnapshotOutputFormat() {
        super(SnapshotFile.class, SnapshotWritable.class);
    }

    public SnapshotOutputFormat(Job job) {
        super(job, SnapshotFile.class, SnapshotWritable.class);
    }

}
