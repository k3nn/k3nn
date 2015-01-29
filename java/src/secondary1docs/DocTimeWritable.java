package secondary1docs;

import io.github.repir.tools.io.buffer.BufferDelayedWriter;
import io.github.repir.tools.io.buffer.BufferReaderWriter;
import io.github.repir.tools.lib.Log;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
/**
 *
 * @author jeroen
 */
public class DocTimeWritable implements Writable {
    String docid;
    int reducer;
    long emittime;
   
    @Override
    public void write(DataOutput d) throws IOException {
       BufferDelayedWriter writer = new BufferDelayedWriter();
       writer.write(docid);
       writer.write(reducer);
       writer.write(emittime);
       writer.writeBuffer(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
       BufferReaderWriter reader = new BufferReaderWriter(di);
       docid = reader.readString();
       reducer = reader.readInt();
       emittime = reader.readLong();
    }
}
