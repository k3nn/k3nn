package wp;

import io.github.repir.tools.hadoop.io.OutputFormat;
import io.github.repir.tools.hadoop.Job;
/**
 *
 * @author jeroen
 */
public class LinkOutputFormat extends OutputFormat<LinkFile, LinkWritable> {

    public LinkOutputFormat() {
        super(LinkFile.class, LinkWritable.class);
    }

    public LinkOutputFormat(Job job) {
        super(job, LinkFile.class, LinkWritable.class);
    }

}
