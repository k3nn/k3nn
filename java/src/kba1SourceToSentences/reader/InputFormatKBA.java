package kba1SourceToSentences.reader;

import io.github.htools.hadoop.InputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import kba1SourceToSentences.kba.StreamItem;

/**
 * Hadoop reader for KBA Streaming corpus, un-gpg-ed and xz compressed
 * @author jeroen
 */
public class InputFormatKBA extends InputFormat<StreamItem> {

    @Override
    public RecordReader<LongWritable, StreamItem> createRecordReader(InputSplit is, TaskAttemptContext tac) {
        return new RecordReaderKBA();
    }

}
