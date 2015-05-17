package kbapool;

import io.github.repir.tools.io.Datafile;
import io.github.repir.tools.lib.Log;
import io.github.repir.tools.hadoop.Conf;
import io.github.repir.tools.hadoop.Job;
import java.util.ArrayList;
import static kbapool.CreatePoolJob.setup;
import static kbapool.CreatePoolJob.temppool;

public class CreatePoolJobMatch {

    private static final Log log = new Log(CreatePoolJobMatch.class);

    public static void main(String[] args) throws Exception {
        Conf conf = new Conf(args, "sentences results createpool creatematch existingpool {existingmatch}");
        //Job job = setup(conf, conf.get("sentences"), conf.get("results"), temppool);
        //if (job.waitForCompletion(true)) {
            Datafile createpool = new Datafile(conf.get("createpool"));
            Datafile creatematch = new Datafile(conf.get("creatematch"));
            Datafile newpooltemp = new Datafile(conf, temppool);
            Datafile existingpool = new Datafile(conf.get("existingpool"));
            ArrayList<Datafile> inematch = new ArrayList();
            if (conf.containsKey("existingmatch")) {
                for (String f : conf.getStrings("existingmatch")) {
                    inematch.add(new Datafile(f));
                }
            }
            new CreatePoolFileMatch(createpool, creatematch, newpooltemp, existingpool, inematch);;
        //}
    }
}
