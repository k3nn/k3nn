package scrape.article;

import io.github.repir.tools.hadoop.io.OutputFormat;
import io.github.repir.tools.hadoop.Job;
/**
 *
 * @author jeroen
 */
public class ArticleOutputFormat extends OutputFormat<ArticleFile, ArticleWritable> {

    public ArticleOutputFormat() {
        super(ArticleFile.class, ArticleWritable.class);
    }

    public ArticleOutputFormat(Job job) {
        super(job, ArticleFile.class, ArticleWritable.class);
    }

}
