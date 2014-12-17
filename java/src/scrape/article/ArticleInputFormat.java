package scrape.article;

import io.github.repir.tools.hadoop.Structured.InputFormat;

public class ArticleInputFormat extends InputFormat<ArticleFile, ArticleWritable> {

    public ArticleInputFormat() {
        super(ArticleFile.class);
    }
}
