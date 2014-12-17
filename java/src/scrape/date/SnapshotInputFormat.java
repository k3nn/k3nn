package scrape.date;

import scrape.article.*;
import io.github.repir.tools.hadoop.Structured.InputFormat;

public class SnapshotInputFormat extends InputFormat<SnapshotFile, SnapshotWritable> {

    public SnapshotInputFormat() {
        super(SnapshotFile.class);
    }
}
