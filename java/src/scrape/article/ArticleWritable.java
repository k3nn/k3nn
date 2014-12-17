package scrape.article;

import io.github.repir.tools.Buffer.BufferDelayedWriter;
import io.github.repir.tools.Buffer.BufferReaderWriter;
import io.github.repir.tools.Lib.MathTools;
import io.github.repir.tools.hadoop.Structured.Writable;
/**
 *
 * @author jeroen
 */
public class ArticleWritable extends Writable<ArticleFile> {
    public long creationtime;
    public String domain;
    public String url;

    public ArticleWritable() {
    }
    
    @Override
    public int hashCode() {
        return MathTools.hashCode(domain.hashCode(), url.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ArticleWritable) {
           ArticleWritable oo = (ArticleWritable) o;
           return oo.url.equals(url) && oo.domain.equals(domain);
        }
        return false;
    }

    @Override
    public void read(ArticleFile f) {
        this.creationtime = f.creationtime.get();
        this.url = f.url.get();
        this.domain = f.domain.get();
    }

    @Override
    public void write(BufferDelayedWriter writer)  {
        writer.write(domain);
        writer.write(url);
        writer.write(creationtime);
    }

    @Override
    public void readFields(BufferReaderWriter reader) {
        domain = reader.readString();
        url = reader.readString();
        creationtime = reader.readLong();
    }

    @Override
    public void write(ArticleFile file) {
        file.domain.set(domain);
        file.creationtime.set(creationtime);
        file.url.set(url);
        file.write();
    }
}
