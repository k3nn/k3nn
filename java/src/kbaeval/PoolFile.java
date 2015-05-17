package kbaeval;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class PoolFile extends File<PoolWritable> {
    IntField query_id = this.addInt("query_id");
    StringField update_id = this.addString("update_id");
    StringField doc_id = this.addString("doc_id");
    IntField sentence_id = this.addInt("sentence_id");
    IntField update_len = this.addInt("update_len");
    StringField duplicate_id = this.addString("duplicate_id");
    StringField update_text = this.addString("update_text");

    public PoolFile(Datafile df) {
        super(df);
        this.hasHeader();
    }

    @Override
    public PoolWritable newRecord() {
        return new PoolWritable();
    }  
}
