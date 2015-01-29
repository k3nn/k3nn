package wp;

import io.github.repir.tools.hadoop.io.StructuredFileInputFormat;

public class LinkInputFormat extends StructuredFileInputFormat<LinkFile, LinkWritable> {

    public LinkInputFormat() {
        super(LinkFile.class);
    }
}
