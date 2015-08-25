package kba1SourceToSentences;

import io.github.htools.lib.DateTools;
import io.github.htools.lib.Log;
import io.github.htools.hadoop.io.ReducerKeys;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import kba1SourceToSentences.kba.StreamItem;

/**
 * generates a short date number from start date 2011-10-01, that is used in the
 * high bits of the internal Node ID allowing to reconstruct the original date.
 * <p/>
 * The use of these keys as reducer numbers is obsolete in this project
 * @author jeroen
 */
public class ReducerKeysDays extends ReducerKeys<Integer> {

    public static final Log log = new Log(ReducerKeysDays.class);
    static final Date firstday = firstDay();
    static final int SECONDSPERDAY = 60 * 60 * 24;

    public ReducerKeysDays(Configuration conf) {
        super(conf);
    }

    @Override
    public Integer getReducerKey(Path path) {
        try {
            String lastDir = path.getParent().getName();
            Date date = DateTools.FORMAT.Y_M_D_H.toDate(lastDir);
            return ReducerKeysDays.getDay(date);
        } catch (ParseException ex) {
            log.fatalexception(ex, "getReducerKey(%s)", path.toString());
        }
        return -1;
    }

    public static int getDay(Date date) {
        return DateTools.diffDays(date, firstday);
    }

    public static int getDay(StreamItem i) {
        return getDay((long) i.getStream_time().getEpoch_ticks());
    }

    public static int getDay(long date) {
        return getDay(DateTools.epochToDate(date));
    }

    public static Date firstDay() {
        try {
            return DateTools.FORMAT.Y_M_D.toDate("2011-10-01");
        } catch (ParseException ex) {
            log.fatalexception(ex, "firstDay()");
        }
        return null;
    }

    @Override
    protected String storeKeys(HashMap<Integer, Integer> keys) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, Integer> entry : keys.entrySet()) {
            sb.append(",").append(entry.getKey()).append("=").append(entry.getValue());
        }
        return sb.deleteCharAt(0).toString();
    }

    @Override
    protected HashMap<Integer, Integer> getStoredKeys(String keys) {
        HashMap<Integer, Integer> map = null;
        if (keys != null) {
            map = new HashMap();
            String entries[] = keys.split(",");
            for (String entry : entries) {
                String part[] = entry.split("=");
                if (part.length == 2) {
                    map.put(Integer.parseInt(part[0]), Integer.parseInt(part[1]));
                }
            }
        }
        return map;
    }
}
