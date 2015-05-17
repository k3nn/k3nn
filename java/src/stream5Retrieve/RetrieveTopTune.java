package stream5Retrieve;

import io.github.repir.tools.collection.ArrayMap3;
import io.github.repir.tools.collection.ArrayMap4;
import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.io.FSPath;
import io.github.repir.tools.lib.ArgsParser;
import static io.github.repir.tools.lib.PrintTools.sprintf;
import io.github.repir.tools.type.Tuple2;
import io.github.repir.tools.type.Tuple3;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import kbaeval.TopicWritable;

public class RetrieveTopTune extends RetrieveTop3File {

    private static final Log log = new Log(RetrieveTopTune.class);

    public RetrieveTopTune(Datafile out) {
        super(out);
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        ArgsParser ap = new ArgsParser(args, "-i input -o output -t topicfile");
        FSPath inpath = new FSPath(ap.get("input"));
        HashMap<Integer, TopicWritable> topics = getTopics(ap.get("topicfile"));

        ArrayMap4<Double, Double, Integer, Integer> settings = new ArrayMap4();
        for (double gainratio : new double[]{0.4, 0.45, 0.5, 0.55, 0.6}) {
            settings.add(gainratio, 1.0, 15, 3);
        }
        for (double hours : new double[]{0.5, 1, 2, 3}) {
            settings.add(0.5, hours, 15, 3);
        }
        for (int length : new int[]{11, 13, 15, 17, 19}) {
            settings.add(0.5, 1.0, length, 3);
        }
        for (int topk : new int[]{1, 2, 3, 4, 5}) {
            settings.add(0.5, 1.0, 15, topk);
        }
        for (Map.Entry<Double, Tuple3<Double, Integer, Integer>> entry : settings) {
            double gainratio = entry.getKey();
            double hours = entry.getValue().key;
            int length = entry.getValue().value;
            int topk = entry.getValue().value2;
            Datafile fout = new Datafile(sprintf("%s.%d.%d.%d.%d", ap.get("output"),
                    (int) (100 * gainratio), (int) (10 * hours), length, topk));
            RetrieveTopTune retriever = new RetrieveTopTune(fout);
            retriever.minInformationGain = gainratio;
            retriever.maxSentenceLengthWords = length;
            retriever.windowRelevanceModelHours = hours;
            retriever.minRankObtained = topk;
            for (TopicWritable t : topics.values()) {
                log.info("%s", inpath.getFilenames());
                ArrayList<String> filenames = inpath.getFilenames("*." + t.id + "$");
                log.info("topic %d %s", t.id, filenames);
                if (filenames.size() == 1) {
                    int digit = Integer.parseInt(filenames.get(0).substring(filenames.get(0).lastIndexOf(".") + 1));
                    if (digit != 6) {
                        Datafile dfin = inpath.getFile(filenames.get(0));
                        retriever.process(t, dfin);
                    }
                }
            }
            retriever.close();
        }
    }
}
