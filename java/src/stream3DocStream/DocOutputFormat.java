package stream3DocStream;

import secondary1docs.*;
import io.github.htools.hadoop.io.OutputFormat;
/**
 *
 * @author jeroen
 */
public class DocOutputFormat extends OutputFormat<DocFile, DocWritable> {

    public DocOutputFormat() {
        super(DocFile.class, DocWritable.class);
    }

}
