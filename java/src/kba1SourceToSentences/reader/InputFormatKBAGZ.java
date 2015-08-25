package kba1SourceToSentences.reader;

import io.github.htools.hadoop.InputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import kba1SourceToSentences.kba.StreamItem;

/**
 * Reader for KBA streaming corpus, version that was repacked into Hadoop sequence files and gzipped
 * @author jeroen
 */
public class InputFormatKBAGZ extends InputFormat<StreamItem> {

    @Override
    public RecordReader<LongWritable, StreamItem> createRecordReader(InputSplit is, TaskAttemptContext tac) {
        return new RecordReaderKBAGZ();
    }

}
