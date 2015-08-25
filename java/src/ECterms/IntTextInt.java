package ECterms;

import io.github.htools.io.buffer.BufferDelayedWriter;
import io.github.htools.io.buffer.BufferReaderWriter;
import io.github.htools.lib.Log;
import io.github.htools.lib.MathTools;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
/**
 *
 * @author jeroen
 */
public class IntTextInt extends IntWritable {
   public static final Log log = new Log( IntTextInt.class );
   public String value1;
   public int value2;
   
   @Override
   public int hashCode() {
       return MathTools.hashCode(get(), value1.hashCode(), value2);
   }
   
   @Override
   public boolean equals(Object o) {
       if (o instanceof IntTextInt) {
           IntTextInt oo = (IntTextInt)o;
           return get() == oo.get() && value2 == oo.value2 && value1.equals(oo.value1);
       }
       return false;
   }
   
    @Override
    public void readFields(DataInput in) throws IOException {
        BufferReaderWriter reader = new BufferReaderWriter(in);
        set(reader.readInt());
        value1 = reader.readString0();
        value2 = reader.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        BufferDelayedWriter writer = new BufferDelayedWriter();
        writer.write(get());
        writer.write0(value1);
        writer.write(value2);
        writer.writeBuffer(out);
    }
}
