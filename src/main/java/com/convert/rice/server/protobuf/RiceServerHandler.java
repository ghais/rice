package com.convert.rice.server.protobuf;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.hbase.TableExistsException;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.joda.time.Interval;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;
import com.convert.rice.TimeSeries;
import com.convert.rice.functions.map.GroupByFunc;
import com.convert.rice.functions.map.Identity;
import com.convert.rice.functions.reduce.Max;
import com.convert.rice.functions.reduce.Mean;
import com.convert.rice.functions.reduce.Median;
import com.convert.rice.functions.reduce.Min;
import com.convert.rice.functions.reduce.ReduceFunction;
import com.convert.rice.functions.reduce.StdDev;
import com.convert.rice.functions.reduce.Sum;
import com.convert.rice.protocol.MapReduce;
import com.convert.rice.protocol.Point;
import com.convert.rice.protocol.Request;
import com.convert.rice.protocol.Request.Create;
import com.convert.rice.protocol.Request.Get;
import com.convert.rice.protocol.Request.Increment;
import com.convert.rice.protocol.Request.Increment.Metric;
import com.convert.rice.protocol.Response;
import com.convert.rice.protocol.Response.CreateResult;
import com.convert.rice.protocol.Response.Error.Builder;
import com.convert.rice.protocol.Response.Error.Status;
import com.convert.rice.protocol.Response.GetResult;
import com.convert.rice.protocol.Response.IncResult;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class RiceServerHandler extends SimpleChannelUpstreamHandler {

    private final EnumMap<MapReduce.ReduceFunction, ReduceFunction> reduceFunctions = new EnumMap<MapReduce.ReduceFunction, ReduceFunction>(
            MapReduce.ReduceFunction.class) {

        private static final long serialVersionUID = -1866459632081142079L;

        {
            put(MapReduce.ReduceFunction.MIN, new Min());
            put(MapReduce.ReduceFunction.MAX, new Max());
            put(MapReduce.ReduceFunction.MEAN, new Mean());
            put(MapReduce.ReduceFunction.MEDIAN, new Median());
            put(MapReduce.ReduceFunction.STD_DEV, new StdDev());
            put(MapReduce.ReduceFunction.SUM, new Sum());
        }
    };

    private final Logger logger = Logger.getLogger(this.getClass().getName());

    private final Timer requests = Metrics.newTimer(RiceProtoBufRpcServer.class, "requests", MILLISECONDS, SECONDS);

    private final Timer increments = Metrics.newTimer(RiceProtoBufRpcServer.class, "increments", MILLISECONDS, SECONDS);

    private final Timer gets = Metrics.newTimer(RiceProtoBufRpcServer.class, "gets", MILLISECONDS, SECONDS);

    private final Timer creates = Metrics.newTimer(RiceProtoBufRpcServer.class, "creates", MILLISECONDS, SECONDS);

    private final ChannelGroup channelGroup;

    private final Supplier<TimeSeries> tsSupplier;

    public RiceServerHandler(ChannelGroup channelGroup, Supplier<TimeSeries> tsSupplier) {
        this.channelGroup = checkNotNull(channelGroup);
        this.tsSupplier = checkNotNull(tsSupplier);
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        channelGroup.add(ctx.getChannel());
        super.channelConnected(ctx, e);
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
            ChannelHandlerContext ctx, MessageEvent event) throws Exception {
        final TimerContext requestTimer = requests.time();
        Response.Builder resopnseBuilder = Response.newBuilder();
        Request req = (Request) event.getMessage();
        TimeSeries timeSeries = tsSupplier.get();
        for (Increment inc : req.getIncList()) {
            final TimerContext timer = increments.time();
            final IncResult.Builder builder = IncResult.newBuilder();
            builder.setKey(inc.getKey()).setType(inc.getType()).setTimestamp(inc.getTimestamp()).build();
            final long timestamp = inc.hasTimestamp() ? inc.getTimestamp() : System.currentTimeMillis();
            final Map<String, Long> metrics = new HashMap<String, Long>(inc.getMetricsCount());
            for (final Metric metric : inc.getMetricsList()) {
                metrics.put(metric.getKey(), metric.getValue());
            }
            try {
                timeSeries.inc(inc.getType(), inc.getKey(), timestamp, metrics);
            } catch (final IOException e) {
                final Response.Error.Builder errorBuilder = Response.Error.newBuilder()
                        .setStatus(Status.IO_EXCEPTION);
                if (null != e.getMessage()) {
                    errorBuilder.setMessage(e.getMessage());
                }
                builder.setError(errorBuilder);
                logger.log(Level.SEVERE, e.getMessage(), e);
            } finally {
                resopnseBuilder.addIncResult(builder);
                timer.stop();
            }
        }
        for (Get get : req.getGetList()) {
            final TimerContext timer = gets.time();
            GetResult.Builder builder = GetResult.newBuilder();
            builder.setKey(get.getKey());
            builder.setType(get.getType());
            try {
                Map<String, DataPoints> dataPoints = timeSeries.get(
                        get.getType(),
                        get.getKey(),
                        new Interval(get.getStart(), get.getEnd()));

                for (DataPoints dps : dataPoints.values()) {
                    DataPoints data = dps;
                    for (MapReduce mr : get.getMapReduceList()) {
                        Function<DataPoints, List<DataPoints>> mapFunction = null;
                        Function<DataPoints, DataPoint> reduceFunction = null;

                        if (mr.getMapFunction().hasGroupBy()) {
                            mapFunction = new GroupByFunc(mr.getMapFunction().getGroupBy());
                        } else if (mr.getMapFunction().hasIdentity()) {
                            mapFunction = new Identity();
                        } else {
                            mapFunction = new Identity();
                        }
                        reduceFunction = reduceFunctions.get(mr.getReduceFunction());

                        data = dps.mapReduce(mapFunction, reduceFunction);
                    }
                    GetResult.Metric.Builder metricBuilder = GetResult.Metric.newBuilder().setName(dps.getMetricName());
                    for (DataPoint dp : data) {
                        metricBuilder.addPoints(
                                Point.newBuilder().setStart(dp.getStart())
                                        .setEnd(dp.getEnd())
                                        .setValue(dp.getValue()));
                    }
                    builder.addMetrics(metricBuilder);
                }
            } catch (IOException e) {
                Response.Error.Builder errorBuilder = Response.Error.newBuilder()
                        .setStatus(Status.IO_EXCEPTION);
                if (null != e.getMessage()) {
                    errorBuilder.setMessage(e.getMessage());
                }
                builder.setError(errorBuilder);
                logger.log(Level.SEVERE, e.getMessage(), e);
            } finally {
                resopnseBuilder.addGetResult(builder);
                timer.stop();
            }
        }

        for (Create create : req.getCreateList()) {
            final TimerContext timer = creates.time();
            CreateResult.Builder builder = CreateResult.newBuilder();
            builder.setType(create.getType());
            try {
                timeSeries.create(create.getType());
            } catch (TableExistsException e) {
                Builder errorBuilder = Response.Error.newBuilder()
                        .setStatus(Status.TABLE_EXISTS);
                if (null != e.getMessage()) {
                    errorBuilder.setMessage(e.getMessage());
                }
                builder.setError(errorBuilder);
                logger.log(Level.SEVERE, e.getMessage(), e);
            } catch (IOException e) {
                Builder errorBuilder = Response.Error.newBuilder()
                        .setStatus(Status.IO_EXCEPTION);
                if (null != e.getMessage()) {
                    errorBuilder.setMessage(e.getMessage());
                }
                builder.setError(errorBuilder);
                logger.log(Level.SEVERE, e.getMessage(), e);
            } finally {
                resopnseBuilder.addCreateResult(builder);
                timer.stop();
            }
        }

        event.getChannel().write(resopnseBuilder.build()).addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                requestTimer.stop();
            }
        });
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        logger.log(
                Level.WARNING,
                "Unexpected exception from downstream.",
                e.getCause());
        channelGroup.remove(e.getChannel());
        e.getChannel().close();
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channelGroup.remove(e.getChannel());
        super.channelClosed(ctx, e);
    }
}
