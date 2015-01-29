package kba9trec;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.io.FSPath;
import io.github.repir.tools.lib.ArgsParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import kbaeval.TopicWritable;

public class RetrieveTopLocal2 extends RetrieveTop {

    private static final Log log = new Log(RetrieveTopLocal2.class);

    public RetrieveTopLocal2(Datafile out, double avgbase, double urltobase) {
        super(out, avgbase, urltobase);
    }

    public static void main(String args[]) throws IOException {
        ArgsParser ap = new ArgsParser(args, "-i input -o output -t topicfile -b avgbase -u urltobase");
        FSPath inpath = new FSPath(ap.get("input"));

        Datafile fout = new Datafile(ap.get("output"));
        double avgbase = ap.getDouble("avgbase", 0.5);
        double urltobase = ap.getDouble("urltobase", 0.5);
        RetrieveTopLocal2 grep = new RetrieveTopLocal2(fout, avgbase, urltobase);
        HashMap<Integer, TopicWritable> topics = getTopics(ap.get("topicfile"));
        for (TopicWritable t : topics.values()) {
            log.info("%s", inpath.getFilenames());
            ArrayList<String> filenames = inpath.getFilenames("*." + t.id + "$");
            log.info("topic %d %s", t.id, filenames);
            if (filenames.size() == 1 && !filenames.get(0).equals("topic.6")) {
                Datafile dfin = inpath.getFile(filenames.get(0));
                grep.grep(t, dfin);
            }
        }
        grep.close();
    }
}
