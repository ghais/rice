package com.convert.rice;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

import java.io.IOException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTablePool;

import com.convert.rice.hbase.HBaseTimeSeries;
import com.convert.rice.server.protobuf.RiceProtoBufRpcServer;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class Main {

    public static void main(String[] args) throws InterruptedException, IOException {
        OptionParser parser = new OptionParser() {

            {
                accepts("zkquorum").withRequiredArg().describedAs("ZooKeeper Quorum").defaultsTo("localhost");
                accepts("port").withRequiredArg().ofType(Integer.class).defaultsTo(5077)
                        .describedAs("Port to start on");

                acceptsAll(asList("help", "h", "?"), "show help");
            }
        };

        OptionSet options = parser.parse(args);
        Configuration conf = new Configuration();
        conf.set(HConstants.ZOOKEEPER_QUORUM, (String) options.valueOf("zkquorum"));
        HTablePool pool = new HTablePool(conf, Integer.MAX_VALUE);
        Supplier<TimeSeries> supplier = Suppliers.memoize(new HBaseTimeSeriesSupplier(conf, pool));
        if (options.has("h")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        new RiceProtoBufRpcServer((Integer) options.valueOf("port"), supplier).start();

    }

    private static class HBaseTimeSeriesSupplier implements Supplier<TimeSeries> {

        private final Configuration conf;

        private final HTablePool pool;

        public HBaseTimeSeriesSupplier(Configuration conf, HTablePool pool) {
            this.conf = checkNotNull(conf);
            this.pool = checkNotNull(pool);
        }

        @Override
        public TimeSeries get() {
            return new HBaseTimeSeries(conf, pool);
        }
    }

}
