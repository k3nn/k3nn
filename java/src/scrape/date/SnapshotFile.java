package scrape.date;

import scrape.article.*;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class SnapshotFile extends File<SnapshotWritable> {

    public StringField creationtime = addString("creationtime");
    public StringField domain = this.addString("domain");
    public StringField url = this.addString("url");

    public SnapshotFile(Datafile df) {
        super(df);
    }

    @Override
    public SnapshotWritable newRecord() {
        return new SnapshotWritable();
    }  
}
