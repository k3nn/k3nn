package kba1SourceToSentences.reader;

import io.github.htools.hadoop.RecordReader;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.ContextTools;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import kba1SourceToSentences.kba.StreamItem;

/**
 * Original reader for 2013 KBA corpus, source must be un-gpg-ed, and is assumed
 * to be still using xz compression.
 * @author jeroen
 */
public class RecordReaderKBA extends RecordReader<StreamItem> {

    public static Log log = new Log(RecordReaderKBA.class);
    private TTransport transport;
    private TBinaryProtocol protocol;

    @Override
    public void initialize(FileSystem filesystem, FileSplit split) throws IOException {
        log.info("initialize %s %d", split.getPath().toString(), System.identityHashCode(this));
        inputstream = RecordReader.getDirectInputStream(filesystem, split);
        inputstream = new XZCompressorInputStream(inputstream);
        initializeThriftReader( inputstream );  
    }

    public void initializeThriftReader(InputStream is) {
        try {
            transport = new TIOStreamTransport(is);
            protocol = new TBinaryProtocol(transport);
            transport.open();
        } catch (TTransportException ex) {
            log.fatalexception(ex, "initialize()");
        } 
    }
    
    @Override
    public boolean nextKeyValue() {
        try {
            record = null;
            StreamItem item = new StreamItem();
            item.read(protocol);
            this.record = item;
            return true;
        } catch (TTransportException ex) {
            if (ex.getType() == TTransportException.END_OF_FILE) {
                log.exception(ex, "nextKeyValue() EOF");
                return false;
            } else if (ex.getType() == TTransportException.UNKNOWN && ContextTools.isLastAttempt(context)) {
                log.exception(ex, "nextKeyValue() Last Unknown");
                return false;
            }
            log.fatalexception(ex, "nextKeyValue() type=%d attempt=%d lastattempt=%b", 
                    ex.getType(), ContextTools.getAttemptID(context), ContextTools.isLastAttempt(context));
        } catch (TException ex) {
            log.fatalexception(ex, "nextKeyValue()");
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        log.info("close %s %d", this.inputstream, System.identityHashCode(this));
        transport.close();
        super.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0.0f;
    }
}
