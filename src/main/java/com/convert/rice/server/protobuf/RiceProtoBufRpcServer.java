package com.convert.rice.server.protobuf;

import static org.jboss.netty.channel.Channels.pipeline;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.joda.time.Interval;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;
import com.convert.rice.TimeSeries;
import com.convert.rice.protocol.Point;
import com.convert.rice.protocol.Request;
import com.convert.rice.protocol.Request.Get;
import com.convert.rice.protocol.Request.Increment;
import com.convert.rice.protocol.Request.Increment.Metric;
import com.convert.rice.protocol.Response;
import com.convert.rice.protocol.Response.GetResult;
import com.convert.rice.protocol.Response.IncResult;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractService;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class RiceProtoBufRpcServer extends AbstractService {

    private final Timer increments = Metrics.newTimer(RiceProtoBufRpcServer.class, "increments", TimeUnit.MILLISECONDS,
            TimeUnit.SECONDS);

    private final Timer gets = Metrics.newTimer(RiceProtoBufRpcServer.class, "gets", TimeUnit.MILLISECONDS,
            TimeUnit.SECONDS);

    private final int port;

    private ServerBootstrap bootstrap;

    public RiceProtoBufRpcServer(int port, Supplier<TimeSeries> tsSupplier) {
        this.port = port;
        // Configure the server.
        this.bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new RiceServerPipelineFactory(tsSupplier));
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
    }

    public void run() {
        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(port));
    }

    private class RiceServerPipelineFactory implements ChannelPipelineFactory {

        private Supplier<TimeSeries> tsSupplier;

        public RiceServerPipelineFactory(Supplier<TimeSeries> tsSupplier) {
            this.tsSupplier = tsSupplier;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline p = pipeline();
            p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
            p.addLast("protobufDecoder", new ProtobufDecoder(Request.getDefaultInstance()));

            p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
            p.addLast("protobufEncoder", new ProtobufEncoder());

            p.addLast("handler", new RiceServerHandler(tsSupplier));
            return p;
        }
    }

    private class RiceServerHandler extends SimpleChannelUpstreamHandler {

        private final Logger logger = Logger.getLogger(this.getClass().getName());

        private Supplier<TimeSeries> tsSupplier;

        public RiceServerHandler(Supplier<TimeSeries> tsSupplier) {
            this.tsSupplier = tsSupplier;
        }

        @Override
        public void handleUpstream(
                ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
            if (e instanceof ChannelStateEvent) {
                logger.info(e.toString());
            }
            super.handleUpstream(ctx, e);
        }

        @Override
        public void messageReceived(
                ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            Request req = (Request) e.getMessage();
            TimeSeries timeSeries = tsSupplier.get();
            if (req.hasInc()) {
                IncResult.Builder builder = IncResult.newBuilder();
                final TimerContext timer = increments.time();
                try {
                    Increment inc = req.getInc();
                    long timestamp = inc.hasTimestamp() ? inc.getTimestamp() : System.currentTimeMillis();
                    Map<String, Long> metrics = new HashMap<String, Long>(inc.getMetricsCount());
                    for (Metric metric : inc.getMetricsList()) {
                        metrics.put(metric.getKey(), metric.getValue());
                    }
                    timeSeries.inc(inc.getType(), inc.getKey(), timestamp, metrics);
                    builder.setKey(inc.getKey()).setType(inc.getType()).setTimestamp(inc.getTimestamp()).build();
                } finally {
                    e.getChannel().write(Response.newBuilder().setIncResult(builder))
                            .addListener(new ChannelFutureListener() {

                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    timer.stop();
                                }
                            });

                }
            } else if (req.hasGet()) {
                final TimerContext timer = gets.time();
                GetResult.Builder builder = GetResult.newBuilder();
                try {
                    Get get = req.getGet();
                    builder.setKey(get.getKey());
                    builder.setType(get.getType());

                    Collection<DataPoints> dataPoints = timeSeries.get(get.getType(), get.getKey(),
                            new Interval(get.getStart(), get.getEnd()), get.getMetricsList());
                    for (DataPoints dps : dataPoints) {
                        GetResult.Metric.Builder metricBuilder = GetResult.Metric.newBuilder().setName(
                                dps.getMetricName());
                        for (DataPoint dp : dps) {
                            metricBuilder.addPoints(Point.newBuilder().setTimestamp(dp.getTimestamp())
                                    .setValue(dp.getValue()));
                        }
                        builder.addMetrics(metricBuilder);
                    }

                } finally {
                    e.getChannel().write(Response.newBuilder().setGetResult(builder))
                            .addListener(new ChannelFutureListener() {

                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    timer.stop();
                                }
                            });

                }
            } else {
                logger.severe("Unknown message type");
            }
        }

        @Override
        public void exceptionCaught(
                ChannelHandlerContext ctx, ExceptionEvent e) {
            logger.log(
                    Level.WARNING,
                    "Unexpected exception from downstream.",
                    e.getCause());
            e.getChannel().close();
        }

    }

    @Override
    protected void doStart() {
        this.run();

    }

    @Override
    protected void doStop() {
        bootstrap.releaseExternalResources();
    }
}
