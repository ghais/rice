package com.convert.rice;

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
        Supplier<TimeSeries> supplier = new HBaseTimeSeriesSupplier(pool);
        if (options.has("h")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        new RiceProtoBufRpcServer((Integer) options.valueOf("protoPort"), supplier).start();

    }

    private static class HBaseTimeSeriesSupplier implements Supplier<TimeSeries> {

        private HTablePool pool;

        public HBaseTimeSeriesSupplier(HTablePool pool) {
            this.pool = pool;
        }

        @Override
        public TimeSeries get() {
            return new HBaseTimeSeries(pool);
        }
    }

}
