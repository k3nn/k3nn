package EC;

import KNN.Stream;
import Sentence.SentenceInputFormat;
import Sentence.SentenceOutputFormat;
import io.github.htools.io.HDFSPath;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.Conf;
import io.github.htools.hadoop.Job;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import Sentence.SentenceWritable;
import io.github.htools.extract.DefaultTokenizer;
import io.github.htools.hadoop.io.IntLongIntWritable;
import io.github.htools.io.Datafile;
import java.util.ArrayList;
import java.util.HashSet;
import kba1SourceToSentences.reader.InputFormatKBA;
import kba1SourceToSentences.reader.InputFormatKBAGZ;
import kbaeval.TopicFile;
import kbaeval.TopicWritable;
import org.apache.hadoop.conf.Configuration;

/**
 * Reads the KBA Streaming corpus (tested with 2013 edition), and writes the 
 * contents as in SentenceFile format. Since we had problems on our cluster
 * processing all KBA files in one jobs, we split jobs per day, writing the output
 * to a single SentenceFile.
 * @author jeroen
 */
public class ECCountJob {
   public static final Log log = new Log( ECCountJob.class );
    static DefaultTokenizer tokenizer = Stream.getUnstemmedTokenizer();

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Conf conf = new Conf(args, "-i input -o output");
        conf.setMapMemoryMB(1024);
        
        Job job = new Job(conf, conf.get("input"), conf.get("output"));
        
        String input = conf.get("input");
        HDFSPath in = new HDFSPath(conf, input);
        job.setInputFormatClass(SentenceInputFormat.class);
        InputFormatKBA.addDirs(job, in);
        
        job.setMapperClass(ECCountMap.class);
        job.setMapOutputKeyClass(IntLongIntWritable.class);
        job.setMapOutputValueClass(SentenceWritable.class);
        
        job.setGroupingComparatorClass(IntLongIntWritable.Comparator.class);
        job.setSortComparatorClass(IntLongIntWritable.SortComparator.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(ECCountReducer.class);
                
        Path out = new Path(conf.get("output"));
        new SentenceOutputFormat(job);
        SentenceOutputFormat.setSingleOutput(job, out);
        
        job.waitForCompletion(false);
    }
   
        /**
     * Read topics from local FS, and store these in the Configuration
     * @param conf 
     */
    public static void setTopics(Conf conf) {
        TopicFile tf = new TopicFile(new Datafile(conf.get("topicfile")));
        for (TopicWritable topic : tf) {
            conf.addArray("topicquery", topic.query);
            conf.addArray("topicid", Integer.toString(topic.id));
            conf.addArray("topicstart", Long.toString(topic.start));
            conf.addArray("topicend", Long.toString(topic.end));
        }
    }

    /**
     * @param conf
     * @return list of topics read from Configuration
     */
    public static ArrayList<TopicWritable> getTopics(Configuration conf) {
        ArrayList<TopicWritable> result = new ArrayList();
        String[] topics = conf.getStrings("topicquery");
        String[] ids = conf.getStrings("topicid");
        String[] starts = conf.getStrings("topicstart");
        String[] ends = conf.getStrings("topicend");
        for (int i = 0; i < topics.length; i++) {
            TopicWritable t = new TopicWritable();
            t.query = topics[i];
            t.id = Integer.parseInt(ids[i]);
            t.start = Long.parseLong(starts[i]);
            t.end = Long.parseLong(ends[i]);
            result.add(t);
        }
        return result;
    }

    /**
     * @param topics
     * @return list of topics, with for each topic a list of unique query terms
     */
    public static ArrayList<HashSet<String>> getTopicTerms(ArrayList<TopicWritable> topics) {
        ArrayList<HashSet<String>> result = new ArrayList();
        for (TopicWritable t : topics) {
            ArrayList<String> tokenize = tokenizer.tokenize(t.query);
            result.add(new HashSet(tokenize));
        }
        return result;
    }

    /**
     * @param topics
     * @return set containing query terms of all topics combined
     */
    public static HashSet<String> allTopicTerms(ArrayList<HashSet<String>> topicterms) {
        HashSet<String> allterms = new HashSet();
        for (HashSet<String> set : topicterms) {
            allterms.addAll(set);
        }
        return allterms;
    }

    
}
