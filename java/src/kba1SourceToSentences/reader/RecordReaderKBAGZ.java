package kba1SourceToSentences.reader;

import io.github.htools.hadoop.RecordReader;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Variant that reads KBA corpus that was un-xz-ed, and then re-gzipped.
 * @author jeroen
 */
public class RecordReaderKBAGZ extends RecordReaderKBA {
    
    @Override
    public void initialize(FileSystem filesystem, FileSplit split) throws IOException {
        inputstream = RecordReader.getInputStream(filesystem, split);
        initializeThriftReader( inputstream );  
    }
}
