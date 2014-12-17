package scrape.article;

import io.github.repir.tools.Content.Datafile;
import io.github.repir.tools.hadoop.Structured.File;

/**
 *
 * @author jeroen
 */
public class ArticleFile extends File<ArticleWritable> {

    public LongField creationtime = addLong("creationtime");
    public StringField domain = this.addString("domain");
    public StringField url = this.addString("url");

    public ArticleFile(Datafile df) {
        super(df);
    }

    @Override
    public ArticleWritable newRecord() {
        return new ArticleWritable();
    }  
}
