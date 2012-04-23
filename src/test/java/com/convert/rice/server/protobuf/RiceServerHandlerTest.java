package com.convert.rice.server.protobuf;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableExistsException;
import org.easymock.EasyMockSupport;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SucceededChannelFuture;
import org.jboss.netty.channel.group.ChannelGroup;
import org.joda.time.Interval;
import org.junit.Test;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;
import com.convert.rice.SeekableView;
import com.convert.rice.TimeSeries;
import com.convert.rice.protocol.Point;
import com.convert.rice.protocol.Request;
import com.convert.rice.protocol.Request.Create;
import com.convert.rice.protocol.Request.Get;
import com.convert.rice.protocol.Request.Increment;
import com.convert.rice.protocol.Response;
import com.convert.rice.protocol.Response.CreateResult;
import com.convert.rice.protocol.Response.Error.Status;
import com.convert.rice.protocol.Response.GetResult;
import com.convert.rice.protocol.Response.IncResult;
import com.google.common.base.Supplier;

public class RiceServerHandlerTest extends EasyMockSupport {

    /**
     * This is a est for a request that has no get/inc/create. It should do nothing.
     * 
     * @throws Exception
     */
    @Test
    public void testMessageReceived_1() throws Exception {
        ChannelGroup channelGroup = createMock(ChannelGroup.class);
        @SuppressWarnings("unchecked")
        Supplier<TimeSeries> tsSupplier = createMock(Supplier.class);
        ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        MessageEvent event = createMock(MessageEvent.class);
        Channel channel = createMock(Channel.class);
        TimeSeries ts = createMock(TimeSeries.class);
        Request request = Request.newBuilder().build();

        Response.Builder responseBuilder = Response.newBuilder();
        expect(event.getChannel()).andReturn(channel).once();
        expect(channel.write(responseBuilder.build())).andReturn(new SucceededChannelFuture(channel));
        expect(event.getMessage()).andReturn(request).once();
        expect(tsSupplier.get()).andReturn(ts).once();

        replayAll();
        RiceServerHandler riceServerHandler = new RiceServerHandler(channelGroup, tsSupplier);
        riceServerHandler.messageReceived(ctx, event);

        verifyAll();
    }

    /**
     * Test receiving a single inc request.
     * 
     * @throws Exception
     */
    @Test
    public void testIncMessageReceived_1() throws Exception {
        String key = "key";
        long timestamp = System.currentTimeMillis();
        String type = "type";
        ChannelGroup channelGroup = createMock(ChannelGroup.class);
        @SuppressWarnings("unchecked")
        Supplier<TimeSeries> tsSupplier = createMock(Supplier.class);
        ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        MessageEvent event = createMock(MessageEvent.class);
        TimeSeries ts = createMock(TimeSeries.class);

        List<Increment.Metric> metrics = new ArrayList<Request.Increment.Metric>();
        for (int i = 0; i < 2; i++) {
            Increment.Metric m = Increment.Metric.newBuilder().setKey(Integer.toString(i)).setValue(i).build();
            metrics.add(m);
        }

        Map<String, Long> dps = new HashMap<String, Long>() {

            private static final long serialVersionUID = 1L;

            {
                put("0", 0L);
                put("1", 1L);
            }
        };
        Increment inc = Increment.newBuilder().setKey(key).setTimestamp(timestamp).setType(type).addAllMetrics(metrics)
                .build();
        Request request = Request.newBuilder().addInc(inc).build();
        ts.inc(type, key, timestamp, dps);

        Response.Builder responseBuilder = Response.newBuilder();
        responseBuilder.addIncResult(IncResult.newBuilder().setKey(key).setTimestamp(timestamp).setType(type));
        Channel channel = createMock(Channel.class);
        expect(event.getChannel()).andReturn(channel).once();
        expect(channel.write(responseBuilder.build())).andReturn(new SucceededChannelFuture(channel));
        expect(event.getMessage()).andReturn(request).once();
        expect(tsSupplier.get()).andReturn(ts).once();

        replayAll();
        RiceServerHandler riceServerHandler = new RiceServerHandler(channelGroup, tsSupplier);
        riceServerHandler.messageReceived(ctx, event);

        verifyAll();
    }

    /**
     * Test receiving a 2 inc request.
     * 
     * @throws Exception
     */
    @Test
    public void testIncMessageReceived_2() throws Exception {
        String key = "key";
        long timestamp = System.currentTimeMillis();
        String type = "type";
        ChannelGroup channelGroup = createMock(ChannelGroup.class);
        @SuppressWarnings("unchecked")
        Supplier<TimeSeries> tsSupplier = createMock(Supplier.class);
        ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        MessageEvent event = createMock(MessageEvent.class);
        TimeSeries ts = createMock(TimeSeries.class);

        List<Increment.Metric> metrics = new ArrayList<Request.Increment.Metric>();
        for (int i = 0; i < 2; i++) {
            Increment.Metric m = Increment.Metric.newBuilder().setKey(Integer.toString(i)).setValue(i).build();
            metrics.add(m);
        }

        Map<String, Long> dps = new HashMap<String, Long>() {

            private static final long serialVersionUID = 1L;

            {
                put("0", 0L);
                put("1", 1L);
            }
        };
        Increment inc1 = Increment.newBuilder().setKey(key).setTimestamp(timestamp).setType(type)
                .addAllMetrics(metrics)
                .build();
        Increment inc2 = Increment.newBuilder().setKey(key).setTimestamp(timestamp).setType(type)
                .addAllMetrics(metrics)
                .build();
        Request request = Request.newBuilder().addInc(inc1).addInc(inc2).build();
        ts.inc(type, key, timestamp, dps);
        ts.inc(type, key, timestamp, dps);

        Response.Builder responseBuilder = Response.newBuilder();
        responseBuilder.addIncResult(IncResult.newBuilder().setKey(key).setTimestamp(timestamp).setType(type))
                .addIncResult(IncResult.newBuilder().setKey(key).setTimestamp(timestamp).setType(type));
        Channel channel = createMock(Channel.class);
        expect(event.getChannel()).andReturn(channel).once();
        expect(channel.write(responseBuilder.build())).andReturn(new SucceededChannelFuture(channel));
        expect(event.getMessage()).andReturn(request).once();
        expect(tsSupplier.get()).andReturn(ts).once();

        replayAll();
        RiceServerHandler riceServerHandler = new RiceServerHandler(channelGroup, tsSupplier);
        riceServerHandler.messageReceived(ctx, event);

        verifyAll();
    }

    /**
     * Test receiving an inc request and throwing an IO excepion.
     * 
     * @throws Exception
     */
    @Test
    public void testIncMessageReceived_3() throws Exception {
        String key = "key";
        long timestamp = System.currentTimeMillis();
        String type = "type";
        ChannelGroup channelGroup = createMock(ChannelGroup.class);
        @SuppressWarnings("unchecked")
        Supplier<TimeSeries> tsSupplier = createMock(Supplier.class);
        ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        MessageEvent event = createMock(MessageEvent.class);
        TimeSeries ts = createMock(TimeSeries.class);

        List<Increment.Metric> metrics = new ArrayList<Request.Increment.Metric>();
        for (int i = 0; i < 2; i++) {
            Increment.Metric m = Increment.Metric.newBuilder().setKey(Integer.toString(i)).setValue(i).build();
            metrics.add(m);
        }

        Map<String, Long> dps = new HashMap<String, Long>() {

            private static final long serialVersionUID = 1L;

            {
                put("0", 0L);
                put("1", 1L);
            }
        };
        Increment inc = Increment.newBuilder().setKey(key).setTimestamp(timestamp).setType(type)
                .addAllMetrics(metrics)
                .build();
        Request request = Request.newBuilder().addInc(inc).build();
        ts.inc(type, key, timestamp, dps);
        IOException e = new IOException("mock exception");
        expectLastCall().andThrow(e);

        Response.Builder responseBuilder = Response.newBuilder();
        responseBuilder.addIncResult(IncResult.newBuilder().setKey(key).setTimestamp(timestamp).setType(type)
                .setError(Response.Error.newBuilder()
                        .setMessage(e.getMessage())
                        .setStatus(Status.IO_EXCEPTION)));
        Channel channel = createMock(Channel.class);
        expect(event.getChannel()).andReturn(channel).once();
        expect(channel.write(responseBuilder.build())).andReturn(new SucceededChannelFuture(channel));
        expect(event.getMessage()).andReturn(request).once();
        expect(tsSupplier.get()).andReturn(ts).once();

        replayAll();
        RiceServerHandler riceServerHandler = new RiceServerHandler(channelGroup, tsSupplier);
        riceServerHandler.messageReceived(ctx, event);

        verifyAll();
    }

    /**
     * Test receiving a create request
     * 
     * @throws Exception
     */
    @Test
    public void testCreateMessageReceived_1() throws Exception {
        String type = "type";
        ChannelGroup channelGroup = createMock(ChannelGroup.class);
        @SuppressWarnings("unchecked")
        Supplier<TimeSeries> tsSupplier = createMock(Supplier.class);
        ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        MessageEvent event = createMock(MessageEvent.class);
        TimeSeries ts = createMock(TimeSeries.class);
        Create create = Create.newBuilder().setType(type).build();

        Request request = Request.newBuilder().addCreate(create).build();
        ts.create(type);
        Response.Builder responseBuilder = Response.newBuilder();
        responseBuilder.addCreateResult(CreateResult.newBuilder().setType(type));
        Channel channel = createMock(Channel.class);
        expect(event.getChannel()).andReturn(channel).once();
        expect(channel.write(responseBuilder.build())).andReturn(new SucceededChannelFuture(channel));
        expect(event.getMessage()).andReturn(request).once();
        expect(tsSupplier.get()).andReturn(ts).once();

        replayAll();
        RiceServerHandler riceServerHandler = new RiceServerHandler(channelGroup, tsSupplier);
        riceServerHandler.messageReceived(ctx, event);

        verifyAll();
    }

    /**
     * Test receiving an create request for a table that already exists.
     * 
     * @throws Exception
     */
    @Test
    public void testCreateMessageReceived_2() throws Exception {
        String type = "type";
        ChannelGroup channelGroup = createMock(ChannelGroup.class);
        @SuppressWarnings("unchecked")
        Supplier<TimeSeries> tsSupplier = createMock(Supplier.class);
        ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        MessageEvent event = createMock(MessageEvent.class);
        TimeSeries ts = createMock(TimeSeries.class);
        Create create = Create.newBuilder().setType(type).build();

        Request request = Request.newBuilder().addCreate(create).build();
        ts.create(type);
        TableExistsException e = new TableExistsException("mock exception");
        expectLastCall().andThrow(e);

        Response.Builder responseBuilder = Response.newBuilder();
        responseBuilder.addCreateResult(CreateResult.newBuilder().setType(type)
                .setError(Response.Error.newBuilder()
                        .setMessage(e.getMessage())
                        .setStatus(Status.TABLE_EXISTS)));
        Channel channel = createMock(Channel.class);
        expect(event.getChannel()).andReturn(channel).once();
        expect(channel.write(responseBuilder.build())).andReturn(new SucceededChannelFuture(channel));
        expect(event.getMessage()).andReturn(request).once();
        expect(tsSupplier.get()).andReturn(ts).once();

        replayAll();
        RiceServerHandler riceServerHandler = new RiceServerHandler(channelGroup, tsSupplier);
        riceServerHandler.messageReceived(ctx, event);

        verifyAll();
    }

    /**
     * Test receiving an create request which causes an IO exception
     * 
     * @throws Exception
     */
    @Test
    public void testCreateMessageReceived_3() throws Exception {
        String type = "type";
        ChannelGroup channelGroup = createMock(ChannelGroup.class);
        @SuppressWarnings("unchecked")
        Supplier<TimeSeries> tsSupplier = createMock(Supplier.class);
        ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        MessageEvent event = createMock(MessageEvent.class);
        TimeSeries ts = createMock(TimeSeries.class);
        Create create = Create.newBuilder().setType(type).build();

        Request request = Request.newBuilder().addCreate(create).build();
        ts.create(type);
        IOException e = new IOException("mock exception");
        expectLastCall().andThrow(e);

        Response.Builder responseBuilder = Response.newBuilder();
        responseBuilder.addCreateResult(CreateResult.newBuilder().setType(type)
                .setError(Response.Error.newBuilder()
                        .setMessage(e.getMessage())
                        .setStatus(Status.IO_EXCEPTION)));
        Channel channel = createMock(Channel.class);
        expect(event.getChannel()).andReturn(channel).once();
        expect(channel.write(responseBuilder.build())).andReturn(new SucceededChannelFuture(channel));
        expect(event.getMessage()).andReturn(request).once();
        expect(tsSupplier.get()).andReturn(ts).once();

        replayAll();
        RiceServerHandler riceServerHandler = new RiceServerHandler(channelGroup, tsSupplier);
        riceServerHandler.messageReceived(ctx, event);

        verifyAll();
    }

    @Test
    public void testGetMessageReceived_1() throws Exception {
        String key = "key";
        long start = 0;
        long end = 1000000;
        String type = "type";
        ChannelGroup channelGroup = createMock(ChannelGroup.class);
        @SuppressWarnings("unchecked")
        Supplier<TimeSeries> tsSupplier = createMock(Supplier.class);
        ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        MessageEvent event = createMock(MessageEvent.class);
        TimeSeries ts = createMock(TimeSeries.class);

        Get get = Get.newBuilder().setType(type).setKey(key).setStart(start).setEnd(end).build();

        Request request = Request.newBuilder().addGet(get).build();
        Map<String, DataPoints> result = new HashMap<String, DataPoints>();
        expect(ts.get(type, key, new Interval(start, end))).andReturn(result);

        Response.Builder responseBuilder = Response.newBuilder();
        responseBuilder.addGetResult(GetResult.newBuilder().setKey(key).setType(type));
        Channel channel = createMock(Channel.class);
        expect(event.getChannel()).andReturn(channel).once();
        expect(channel.write(responseBuilder.build())).andReturn(new SucceededChannelFuture(channel));
        expect(event.getMessage()).andReturn(request).once();
        expect(tsSupplier.get()).andReturn(ts).once();

        replayAll();
        RiceServerHandler riceServerHandler = new RiceServerHandler(channelGroup, tsSupplier);
        riceServerHandler.messageReceived(ctx, event);

        verifyAll();
    }

    @Test
    public void testGetMessageReceived_2() throws Exception {
        String key = "key";
        long start = 0;
        long end = 1000000;
        String type = "type";
        ChannelGroup channelGroup = createMock(ChannelGroup.class);
        @SuppressWarnings("unchecked")
        Supplier<TimeSeries> tsSupplier = createMock(Supplier.class);
        ChannelHandlerContext ctx = createMock(ChannelHandlerContext.class);
        MessageEvent event = createMock(MessageEvent.class);
        TimeSeries ts = createMock(TimeSeries.class);

        Get get = Get.newBuilder().setType(type).setKey(key).setStart(start).setEnd(end).build();

        Request request = Request.newBuilder().addGet(get).build();
        DataPoints dps = createMock(DataPoints.class);
        expect(dps.getMetricName()).andReturn("x");
        SeekableView iterator = createMock(SeekableView.class);
        expect(dps.iterator()).andReturn(iterator).once();
        expect(iterator.hasNext()).andReturn(true).once();
        expect(iterator.next()).andReturn(new DataPoint(0, 1000, 1));
        expect(iterator.hasNext()).andReturn(false).once();

        Map<String, DataPoints> result = new HashMap<String, DataPoints>();
        result.put("x", dps);
        expect(ts.get(type, key, new Interval(start, end))).andReturn(result);

        Response.Builder responseBuilder = Response.newBuilder();
        responseBuilder.addGetResult(GetResult
                .newBuilder()
                .setKey(key)
                .setType(type)
                .addMetrics(
                        GetResult.Metric.newBuilder().setName("x")
                                .addPoints(Point.newBuilder().setEnd(1000).setStart(0).setValue(1))));
        Channel channel = createMock(Channel.class);
        expect(event.getChannel()).andReturn(channel).once();
        expect(channel.write(responseBuilder.build())).andReturn(new SucceededChannelFuture(channel));
        expect(event.getMessage()).andReturn(request).once();
        expect(tsSupplier.get()).andReturn(ts).once();

        replayAll();
        RiceServerHandler riceServerHandler = new RiceServerHandler(channelGroup, tsSupplier);
        riceServerHandler.messageReceived(ctx, event);

        verifyAll();
    }
}
