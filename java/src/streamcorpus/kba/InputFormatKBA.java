package streamcorpus.kba;

import io.github.repir.tools.hadoop.InputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import streamcorpus.StreamItem;
/**
 *
 * @author jeroen
 */
public class InputFormatKBA extends InputFormat<StreamItem> {

    @Override
    public RecordReader<LongWritable, StreamItem> createRecordReader(InputSplit is, TaskAttemptContext tac) {
        return new RecordReaderKBA();
    }

}
