package scrape.page;

import io.github.repir.tools.hadoop.IO.OutputFormatFolder;
import io.github.repir.tools.hadoop.Job;
/**
 *
 * @author jeroen
 */
public class PageOutputFormat extends OutputFormatFolder<PageFile, PageWritable> {

    public PageOutputFormat() {
        super(PageFile.class, PageWritable.class);
    }

    public PageOutputFormat(Job job) {
        super(job, PageFile.class, PageWritable.class);
    }

}
