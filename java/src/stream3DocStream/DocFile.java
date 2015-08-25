package stream3DocStream;

import io.github.htools.io.Datafile;
import io.github.htools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class DocFile extends File<DocWritable> {

    public StringField docid = this.addString("docid");
    public LongField creationtime = this.addLong("creationtime");
    public BoolField isCandidate = this.addBoolean("iscandidate");

    public DocFile(Datafile df) {
        super(df);
    }

    @Override
    public DocWritable newRecord() {
        return new DocWritable();
    }  
}
