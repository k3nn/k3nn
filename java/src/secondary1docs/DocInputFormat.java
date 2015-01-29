package secondary1docs;

import io.github.repir.tools.hadoop.io.StructuredFileInputFormat;

public class DocInputFormat extends StructuredFileInputFormat<DocFile, DocWritable> {

    public DocInputFormat() {
        super(DocFile.class);
    }
}
