package scrape.date;

import scrape.article.*;
import io.github.repir.tools.hadoop.io.InputFormat;
import io.github.repir.tools.hadoop.io.StructuredFileInputFormat;

public class SnapshotInputFormat extends StructuredFileInputFormat<SnapshotFile, SnapshotWritable> {

    public SnapshotInputFormat() {
        super(SnapshotFile.class);
    }
}
