package secondary3Fast;

import MatchingClusterNode.MatchingClusterNodeWritable;
import Cluster.ClusterFile;
import Cluster.ClusterWritable;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import io.github.repir.tools.hadoop.io.ParamFileInputFormat;
import io.github.repir.tools.io.EOCException;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.io.buffer.BufferDelayedWriter;
import io.github.repir.tools.io.buffer.BufferReaderWriter;
import io.github.repir.tools.io.buffer.BufferSerializable;
import io.github.repir.tools.io.struct.StructureReader;
import io.github.repir.tools.io.struct.StructureWriter;
import io.github.repir.tools.lib.MathTools;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import static stream5Retrieve.RetrieveTop3.getTopics;
import kbaeval.TopicWritable;
import kbaeval.TrecWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class RetrieveJob {

    private static final Log log = new Log(RetrieveJob.class);
    static Conf conf; 

    public static Job setup(String args[]) throws IOException {
        conf = new Conf(args, "-i input -o output -t topicfile");
        conf.setReduceSpeculativeExecution(false);
        conf.setMapSpeculativeExecution(false);
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(3600000);

        String input = conf.get("input");
        Path out = new Path(conf.get("output"));

        log.info("Tool name: %s", log.getLoggedClass().getName());
        log.info(" - input: %s", input);
        log.info(" - output: %s", out);
        log.info(" - topicfile: %s", conf.get("topicfile"));

        Job job = new Job(conf, input, out, conf.get("topicfile"));
        //job.getConfiguration().setInt("mapreduce.task.timeout", 1800000);

        job.setInputFormatClass(InputFormat.class);
        addInput(job, new HDFSPath(conf, conf.get("input")), conf.get("topicfile"));

        job.setNumReduceTasks(getParams().size());
        job.setMapperClass(RetrieveMap.class);
        job.setReducerClass(RetrieveReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(Setting.class);
        job.setMapOutputValueClass(MatchingClusterNodeWritable.class);
        job.setPartitionerClass(SettingPartitioner.class);

        return job;
    }

    public static void addInput(Job job, HDFSPath input, String topicfile) throws IOException {
        HashMap<Integer, TopicWritable> topics = getTopics(topicfile);
        for (Datafile df : input.getFiles()) {
            String digit = df.getFilename().substring(df.getFilename().lastIndexOf(".") + 1);
            if (digit.length() > 0) {
                int topicid = Integer.parseInt(digit);
                if (topicid != 23) {
                    TopicWritable t = topics.get(topicid);
                    Collection<Setting> params = getParams();
                    for (Setting s : params) {
                        s.topicid = t.id;
                        s.topicstart = t.start;
                        s.topicend = t.end;
                        s.query = t.query;
                        InputFormat.add(job, s, new Path(df.getCanonicalPath()));
                    }
                }
            }
        }
    }

    static class InputFormat extends ParamFileInputFormat<ClusterFile, Setting, ClusterWritable> {

        public InputFormat() {
            super(ClusterFile.class);
        }

        @Override
        protected Setting createKey() {
            return new Setting();
        }

    }

    public static class Setting implements WritableComparable<Setting>, BufferSerializable {

        public double gainratio = 0.5;
        public double hours = 1.0;
        public int length = 20;
        public int topk = 5;
        public String query;
        public int topicid;
        public long topicstart;
        public long topicend;

        @Override
        public int hashCode() {
            return MathTools.hashCode(length, topk, (int) (gainratio * 100), (int) (hours * 10));
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Setting) {
                Setting s = (Setting) o;
                return gainratio == s.gainratio && hours == s.hours && length == s.length && topk == s.topk;
            }
            return false;
        }

        @Override
        public void read(StructureReader reader) throws EOCException {
            gainratio = reader.readDouble();
            hours = reader.readDouble();
            length = reader.readInt();
            topk = reader.readInt();
            topicid = reader.readInt();
            topicstart = reader.readLong();
            topicend = reader.readLong();
            query = reader.readString();
        }

        @Override
        public void write(StructureWriter writer) {
            writer.write(gainratio);
            writer.write(hours);
            writer.write(length);
            writer.write(topk);
            writer.write(topicid);
            writer.write(topicstart);
            writer.write(topicend);
            writer.write(query);
        }

        @Override
        public void write(DataOutput d) throws IOException {
            BufferDelayedWriter writer = new BufferDelayedWriter();
            write(writer);
            writer.writeBuffer(d);
        }

        @Override
        public void readFields(DataInput di) throws IOException {
            BufferReaderWriter reader = new BufferReaderWriter(di);
            this.read(reader);
        }

        @Override
        public int compareTo(Setting o) {
            int comp = topicid - o.topicid;
            return comp;
        }
    }

    static class SettingPartitioner extends Partitioner<Setting, Object> {

        HashMap<Setting, Integer> params = new HashMap();

        public SettingPartitioner() {
            for (Setting s : getParams()) {
                params.put(s, params.size());
            }
        }

        @Override
        public int getPartition(Setting key, Object value, int i) {
            return params.get(key);
        }
    }

    public static ArrayList<Setting> getParams() {
        ArrayList<Setting> settings = new ArrayList();
        settings.add(new Setting());
        return settings;
    }

    public static void main(String[] args) throws Exception {
        Job job = setup(args);
        job.waitForCompletion(true);
    }
}
