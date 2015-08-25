package secondary1docs;

import io.github.htools.io.Datafile;
import io.github.htools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class DocFile extends File<DocWritable> {

    public StringField docid = this.addString("docid");
    public LongField emittime = this.addLong("emittime");

    public DocFile(Datafile df) {
        super(df);
    }

    @Override
    public DocWritable newRecord() {
        return new DocWritable();
    }  
}
