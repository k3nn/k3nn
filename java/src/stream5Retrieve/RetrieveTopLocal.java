package stream5Retrieve;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.io.FSPath;
import io.github.repir.tools.lib.ArgsParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import kbaeval.TopicWritable;

public class RetrieveTopLocal extends RetrieveTop3File {

    private static final Log log = new Log(RetrieveTopLocal.class);

    public RetrieveTopLocal(Datafile out) {
        super(out);
        this.windowRelevanceModelHours = 1.0;
        this.maxSentenceLengthWords = 20;
        this.minInformationGain = 0.5;
        this.minRankObtained = 5;
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        ArgsParser ap = new ArgsParser(args, "-i input -o output -t topicfile -k [series]");
        FSPath inpath = new FSPath(ap.get("input"));

        Datafile fout = new Datafile(ap.get("output"));
        log.info("output %s", ap.get("output"));
        RetrieveTopLocal grep = new RetrieveTopLocal(fout);
        HashMap<Integer, TopicWritable> topics = getTopics(ap.get("topicfile"));         
        for (TopicWritable t : topics.values()) {
            log.info("%s", inpath.getFilenames());
            ArrayList<String> filenames = inpath.getFilenames("*." + t.id + "$");
            log.info("topic %d %s", t.id, filenames);
            if (filenames.size() == 1) {
                if (ap.exists("series") && !ap.get("series").contains(Integer.toString(t.id))) {
                    continue;
                }
                Datafile dfin = inpath.getFile(filenames.get(0));
                grep.process(t, dfin);
            }
        }
        grep.close();
    }
}
