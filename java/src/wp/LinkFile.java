package wp;

import scrape.article.*;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.tsv.File;

/**
 *
 * @author jeroen
 */
public class LinkFile extends File<LinkWritable> {

    public StringField entity = this.addString("entity");
    public StringField anchortext = this.addString("anchortext");
    public IntField frequency = addInt("frequency");

    public LinkFile(Datafile df) {
        super(df);
    }

    @Override
    public LinkWritable newRecord() {
        return new LinkWritable();
    }  
}
