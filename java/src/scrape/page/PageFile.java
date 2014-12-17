package scrape.page;

import io.github.repir.tools.Content.Datafile;
import scrape.article.ArticleFile;

/**
 *
 * @author jeroen
 */
public class PageFile extends ArticleFile {

    public PageFile(Datafile df) {
        super(df);
    }

    @Override
    public PageWritable newRecord() {
        return new PageWritable();
    }  
}
