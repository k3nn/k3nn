package scrape.page;

import io.github.repir.tools.io.buffer.BufferDelayedWriter;
import io.github.repir.tools.io.buffer.BufferReaderWriter;
import io.github.repir.tools.hadoop.structured.Writable;

/**
 *
 * @author jeroen
 */
public class PageWritable extends Writable<PageFile> {

    public long creationtime;
    public String url;
    public byte[] content;

    @Override
    public void write(BufferDelayedWriter writer) {
        writer.write(creationtime);
        writer.write(url);
        writer.write(content);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        creationtime = reader.readLong();
        url = reader.readString();
        content = reader.readByteArray();
    }

    @Override
    public void read(PageFile file) {
        creationtime = file.creationtime.value;
        url = file.url.value;
        content = file.content.value;
    }

    @Override
    public void write(PageFile file) {
        file.creationtime.write(creationtime);
        file.url.write(url);
        file.content.write(content);
    }
}
