package secondary1docs;

import Cluster.*;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.tsv.File;

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
