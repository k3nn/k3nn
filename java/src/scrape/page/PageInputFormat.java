package scrape.page;

import io.github.repir.tools.hadoop.io.InputFormat;
import io.github.repir.tools.hadoop.io.StructuredFileInputFormat;

public class PageInputFormat extends StructuredFileInputFormat<PageFile, PageWritable> {

    public PageInputFormat() {
        super(PageFile.class);
    }
}
