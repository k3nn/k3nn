package kba1SourceToSentences;

import Sentence.SentenceOutputFormat;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.io.IntLongStringIntWritable;
import io.github.repir.tools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import Sentence.SentenceWritable;
import io.github.repir.tools.hadoop.io.IntLongIntWritable;
import kba1SourceToSentences.reader.InputFormatKBA;
import kba1SourceToSentences.reader.InputFormatKBAGZ;

/**
 * Reads the KBA Streaming corpus (tested with 2013 edition), and writes the 
 * contents as in SentenceFile format. Since we had problems on our cluster
 * processing all KBA files in one jobs, we split jobs per day, writing the output
 * to a single SentenceFile.
 * @author jeroen
 */
public class SourceToSentenceJob {
   public static final Log log = new Log( SourceToSentenceJob.class );

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(8192);
        
        Job job = new Job(conf, conf.get("input"), conf.get("output"));
        
        String input = conf.get("input");
        HDFSPath in = new HDFSPath(conf, input);
        job.setInputFormatClass(InputFormatKBAGZ.class);
        InputFormatKBA.addDirs(job, in);
        
        job.setMapperClass(SourceToSentenceMap.class);
        job.setMapOutputKeyClass(IntLongIntWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        
        job.setGroupingComparatorClass(IntLongIntWritable.Comparator.class);
        job.setSortComparatorClass(IntLongIntWritable.SortComparator.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(SourceToSentenceReducer.class);
                
        Path out = new Path(conf.get("output"));
        new SentenceOutputFormat(job);
        SentenceOutputFormat.setSingleOutput(job, out);
        
        job.waitForCompletion(false);
    }
   
}
