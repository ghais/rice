package com.convert.rice.server.protobuf;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.jboss.netty.channel.Channels.pipeline;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import com.convert.rice.TimeSeries;
import com.convert.rice.protocol.Request;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractService;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

public class RiceProtoBufRpcServer extends AbstractService {

    private final int port;

    private final ServerBootstrap bootstrap;

    private final ChannelGroup channelGroup;

    private final Supplier<TimeSeries> tsSupplier;

    public RiceProtoBufRpcServer(int port, Supplier<TimeSeries> tsSupplier) {
        this.tsSupplier = checkNotNull(tsSupplier);
        this.port = port;
        this.channelGroup = new DefaultChannelGroup(this.getClass().getName());
        // Configure the server.
        this.bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new RiceServerPipelineFactory());
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("reuseAddress", true);

        Metrics.newGauge(RiceProtoBufRpcServer.class, "open-channels", new Gauge<Integer>() {

            @Override
            public Integer value() {
                return channelGroup.size();
            }
        });
    }

    public void run() {
        // Bind and start to accept incoming connections.
        channelGroup.add(bootstrap.bind(new InetSocketAddress(port)));
    }

    class RiceServerPipelineFactory implements ChannelPipelineFactory {

        public RiceServerPipelineFactory() {
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline p = pipeline();
            p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
            p.addLast("protobufDecoder", new ProtobufDecoder(Request.getDefaultInstance()));

            p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
            p.addLast("protobufEncoder", new ProtobufEncoder());

            p.addLast("handler", new RiceServerHandler(channelGroup, tsSupplier));
            return p;
        }
    }

    @Override
    protected void doStart() {
        try {
            this.run();
            this.notifyStarted();
        } catch (Throwable t) {
            this.notifyFailed(t);
        }
    }

    @Override
    protected void doStop() {
        channelGroup.close();
        bootstrap.releaseExternalResources();
        this.notifyStopped();
    }
}
