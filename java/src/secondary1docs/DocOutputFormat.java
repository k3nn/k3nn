package secondary1docs;

import io.github.repir.tools.hadoop.io.OutputFormat;
/**
 *
 * @author jeroen
 */
public class DocOutputFormat extends OutputFormat<DocFile, DocWritable> {

    public DocOutputFormat() {
        super(DocFile.class, DocWritable.class);
    }

}
