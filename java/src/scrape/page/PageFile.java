package scrape.page;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.hadoop.structured.File;

/**
 *
 * @author jeroen
 */
public class PageFile extends File<PageWritable> {
    public LongField creationtime = addLong("creationtime");
    public StringField url = this.addString("url");
    public MemField content = this.addMem("content");

    public PageFile(Datafile df) {
        super(df);
    }

    @Override
    public PageWritable newRecord() {
        return new PageWritable();
    }  
}
