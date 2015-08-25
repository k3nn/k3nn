package stream3DocStream;

import secondary1docs.*;
import io.github.htools.hadoop.io.StructuredFileInputFormat;

public class DocInputFormat extends StructuredFileInputFormat<DocFile, DocWritable> {

    public DocInputFormat() {
        super(DocFile.class);
    }
}
