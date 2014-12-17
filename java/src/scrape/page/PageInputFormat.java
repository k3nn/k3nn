package scrape.page;

import io.github.repir.tools.hadoop.Structured.InputFormat;

public class PageInputFormat extends InputFormat<PageFile, PageWritable> {

    public PageInputFormat() {
        super(PageFile.class);
    }
}
