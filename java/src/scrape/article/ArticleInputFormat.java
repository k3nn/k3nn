package scrape.article;

import io.github.repir.tools.hadoop.io.InputFormat;
import io.github.repir.tools.hadoop.io.StructuredFileInputFormat;

public class ArticleInputFormat extends StructuredFileInputFormat<ArticleFile, ArticleWritable> {

    public ArticleInputFormat() {
        super(ArticleFile.class);
    }
}
