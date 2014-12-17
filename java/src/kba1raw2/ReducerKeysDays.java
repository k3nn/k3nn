package kba1raw2;

import kba1raw.*;
import io.github.repir.tools.Lib.DateTools;
import io.github.repir.tools.Lib.Log;
import io.github.repir.tools.hadoop.IO.ReducerKeys;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import streamcorpus.StreamItem;

/**
 *
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
            Date date = DateTools.FORMAT.Y_M_D_H.parse(lastDir);
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
        return getDay(DateTools.secToDate(date));
    }

    public long getreducerKey(StreamItem i) {
        return this.getreducerKey(i);
    }

    public static Date firstDay() {
        try {
            return DateTools.FORMAT.Y_M_D.parse("2011-10-01");
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

    public static void main(String[] args) throws ParseException {
        Date d1 = DateTools.FORMAT.Y_M_D.parse("2011-10-31");
        Date d2 = DateTools.FORMAT.Y_M_D.parse("2011-11-01");
        log.info("%d %d %d", d2.getTime() / 1000, getDay(1320105540) << 22, getDay(1320105660) << 22);
    }
}
