package com.convert.rice;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static org.apache.commons.lang.StringUtils.equalsIgnoreCase;

import java.io.IOException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTablePool;

import com.convert.rice.hbase.HBaseTimeSeries;
import com.convert.rice.protocol.Aggregation;
import com.convert.rice.server.protobuf.RiceProtoBufRpcServer;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class Main {

    public static void main(String[] args) throws InterruptedException, IOException {
        OptionParser parser = new OptionParser() {

            {
                accepts("zkquorum")
                        .withRequiredArg()
                        .describedAs("ZooKeeper Quorum")
                        .defaultsTo("localhost");
                accepts("port")
                        .withRequiredArg()
                        .ofType(Integer.class)
                        .defaultsTo(5077)
                        .describedAs("Port to start on");
                accepts("hbase.writeToWAL")
                        .withRequiredArg()
                        .describedAs("Whether to write increments to the WAL first (true or false)")
                        .ofType(Boolean.class)
                        .defaultsTo(true);
                accepts("minInterval")
                        .withRequiredArg()
                        .describedAs("the minimum interval to store (s for second, m for minute,h for hour")
                        .ofType(String.class)
                        .defaultsTo("m");

                acceptsAll(asList("help", "h", "?"), "show help");
            }
        };

        OptionSet options = parser.parse(args);

        if (options.has("h")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Configuration conf = new Configuration();
        conf.set(HConstants.ZOOKEEPER_QUORUM, (String) options.valueOf("zkquorum"));
        HTablePool pool = new HTablePool(conf, Integer.MAX_VALUE);
        Boolean writeToWAL = (Boolean) options.valueOf("hbase.writeToWAL");

        Aggregation precision = equalsIgnoreCase("s", (String) options.valueOf("minInterval")) ? Aggregation.SECOND :
                equalsIgnoreCase((String) options.valueOf("minInterval"), "m") ? Aggregation.MINUTE :
                        Aggregation.HOUR;
        HBaseTimeSeriesSupplier hbaseTimeSeriesSupplier = new HBaseTimeSeriesSupplier(conf, pool, writeToWAL, precision);
        Supplier<TimeSeries> supplier = Suppliers.memoize(hbaseTimeSeriesSupplier);

        new RiceProtoBufRpcServer((Integer) options.valueOf("port"), supplier).start();

    }

    private static class HBaseTimeSeriesSupplier implements Supplier<TimeSeries> {

        private final Configuration conf;

        private final HTablePool pool;

        private final boolean writeToWAL;

        private final Aggregation aggregation;

        public HBaseTimeSeriesSupplier(Configuration conf, HTablePool pool, boolean writeToWAL, Aggregation aggregation) {
            this.conf = checkNotNull(conf);
            this.pool = checkNotNull(pool);
            this.writeToWAL = writeToWAL;
            this.aggregation = checkNotNull(aggregation);
        }

        @Override
        public TimeSeries get() {
            return new HBaseTimeSeries(conf, pool, writeToWAL, aggregation);
        }
    }

}
