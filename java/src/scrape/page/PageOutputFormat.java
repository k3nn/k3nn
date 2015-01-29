package scrape.page;

import io.github.repir.tools.hadoop.io.OutputFormat;
import io.github.repir.tools.hadoop.Job;
/**
 *
 * @author jeroen
 */
public class PageOutputFormat extends OutputFormat<PageFile, PageWritable> {

    public PageOutputFormat() {
        super(PageFile.class, PageWritable.class);
    }

    public PageOutputFormat(Job job) {
        super(job, PageFile.class, PageWritable.class);
    }

}
