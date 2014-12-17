package scrape.article;

import io.github.repir.tools.hadoop.IO.OutputFormatFolder;
import io.github.repir.tools.hadoop.Job;
/**
 *
 * @author jeroen
 */
public class ArticleOutputFormat extends OutputFormatFolder<ArticleFile, ArticleWritable> {

    public ArticleOutputFormat() {
        super(ArticleFile.class, ArticleWritable.class);
    }

    public ArticleOutputFormat(Job job) {
        super(job, ArticleFile.class, ArticleWritable.class);
    }

}
