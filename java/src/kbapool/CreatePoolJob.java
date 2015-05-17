package kbapool;

import Sentence.SentenceInputFormat;
import Sentence.SentenceWritable;
import io.github.repir.tools.collection.HashMap3;
import io.github.repir.tools.collection.HashMapSet;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import io.github.repir.tools.hadoop.io.IntLongWritable;
import io.github.repir.tools.io.FSPath;
import io.github.repir.tools.io.HDFSPath;
import io.github.repir.tools.type.KV;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import kbaeval.PoolWritable;
import kbaeval.TrecFile;
import kbaeval.TrecWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class CreatePoolJob {

    private static final Log log = new Log(CreatePoolJob.class);
    static Conf conf;
    public static String temppool = "input/newpool";

    public static Job setup(Conf conf, String sentences, String results, String poolfile) throws IOException {
        conf.setReduceSpeculativeExecution(false);
        conf.setMapSpeculativeExecution(false);
        conf.setMapMemoryMB(4096);
        conf.setTaskTimeout(3600000);
        conf.set("poolfile", poolfile);
        conf.set("results", results);

        Job job = new Job(conf, sentences, results, poolfile);
        //job.getConfiguration().setInt("mapreduce.task.timeout", 1800000);
        set(conf);
        job.setInputFormatClass(SentenceInputFormat.class);
        SentenceInputFormat.addInputPath(job, new HDFSPath(conf, sentences));

        job.setNumReduceTasks(1);
        job.setMapperClass(CreatePoolMap.class);
        job.setReducerClass(CreatePoolReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(PoolWritable.class);

        return job;
    }

    public static void set(Conf conf) throws IOException {
        HashMap<String, Item> map = new HashMap();
        String paths[] = conf.getStrings("results");
        for (String p : paths) {
            HDFSPath path = new HDFSPath(conf, p);
            for (Datafile df : path.getFiles()) {
                if (!df.getFilename().endsWith(".titles")) {
                    log.info("%s", df.getFilename());
                    TrecFile tf = new TrecFile(df);
                    for (TrecWritable w : tf) {
                        Item item = new Item(w.document, w.sentence, w.topic);
                        map.put(item.getKey(), item);
                    }
                }
            }
        }
        Datafile df = new Datafile(conf, "input/poollisttemp");
        for (Map.Entry<String, Item> entry : map.entrySet()) {
            Item item = entry.getValue();
            df.printf("%s %d %d\n", item.update, item.linenr, item.querynr);
            log.info("%s", item.update);
        }
        df.closeWrite();
    }

    public static HashMap<String, Item> get(Conf conf) throws IOException {
        HashMap<String, Item> map = new HashMap();
        Datafile df = new Datafile(conf, "input/poollisttemp");
        String content[] = df.readAsString().split("\n");
        for (String line : content) {
            String parts[] = line.split(" ");
            Item item = new Item(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
            map.put(item.getKey(), item);
        }
        return map;
    }

    public static class Item {

        public String update;
        public int linenr;
        public int querynr;

        public Item(String update, int linenr, int querynr) {
            this.update = update;
            this.linenr = linenr;
            this.querynr = querynr;
        }

        public String getKey() {
            return update + "-" + linenr;
        }

        public static String toItemKey(SentenceWritable w) {
            return w.getDocumentID() + "-" + w.sentenceNumber;
        }
    }

    public static void main(String[] args) throws Exception {
        Conf conf = new Conf(args, "sentences results createpool creatematch {existingmatch}");
        Job job = setup(conf, conf.get("sentences"), conf.get("results"), temppool);
        if (job.waitForCompletion(true)) {
            Datafile createpool = new Datafile(conf.get("createpool"));
            Datafile creatematch = new Datafile(conf.get("creatematch"));
            Datafile newpooltemp = new Datafile(conf, temppool);
            ArrayList<Datafile> inematch = new ArrayList();
            if (conf.containsKey("existingmatch")) {
                for (String f : conf.getStrings("existingmatch")) {
                    inematch.add(new Datafile(f));
                }
            }
            new CreatePoolFile(createpool, creatematch, newpooltemp, inematch);;
        }
    }
}
