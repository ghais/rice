package com.convert.rice.server.http;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.jboss.netty.channel.Channels.pipeline;
import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
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
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.util.CharsetUtil;
import org.joda.time.Interval;

import com.convert.rice.DataPoint;
import com.convert.rice.DataPoints;
import com.convert.rice.TimeSeries;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class RiceHttpServer extends AbstractService {

    private final Type listType = new TypeToken<List<String>>() {
    }.getType();

    private final Type mapType = new TypeToken<Map<String, Long>>() {
    }.getType();

    private final Timer increments = Metrics.newTimer(RiceHttpServer.class, "increments", TimeUnit.MILLISECONDS,
            TimeUnit.SECONDS);

    private final Timer gets = Metrics.newTimer(RiceHttpServer.class, "gets", TimeUnit.MILLISECONDS,
            TimeUnit.SECONDS);

    private final Gson gson = new GsonBuilder().registerTypeAdapter(DataPoint.class, new TypeAdapter<DataPoint>() {

        @Override
        public void write(JsonWriter out, DataPoint dp) throws IOException {
            out.beginArray();
            out.value(dp.getTimestamp());
            out.value(dp.getValue());
            out.endArray();
        }

        @Override
        public DataPoint read(JsonReader in) throws IOException {
            in.beginArray();
            DataPoint dp = new DataPoint(in.nextLong(), in.nextLong());
            in.endArray();
            return dp;
        }
    }).create();

    private final int port;

    private final ServerBootstrap bootstrap;

    private final Supplier<TimeSeries> tsSupplier;

    public RiceHttpServer(int port, Supplier<TimeSeries> tsSupplier) {
        this.port = port;
        this.tsSupplier = checkNotNull(tsSupplier);
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

    }

    public void run() {
        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(port));
    }

    private class RiceServerPipelineFactory implements ChannelPipelineFactory {

        public RiceServerPipelineFactory() {
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = pipeline();
            pipeline.addLast("log", new LoggingHandler());

            // Uncomment the following line if you want HTTPS
            // SSLEngine engine = SecureChatSslContextFactory.getServerContext().createSSLEngine();
            // engine.setUseClientMode(false);
            // pipeline.addLast("ssl", new SslHandler(engine));

            pipeline.addLast("decoder", new HttpRequestDecoder());
            // Uncomment the following line if you don't want to handle HttpChunks.
            // pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
            pipeline.addLast("encoder", new HttpResponseEncoder());
            // Remove the following line if you don't want automatic content compression.
            pipeline.addLast("deflater", new HttpContentCompressor());
            // pipeline.addLast("compactor", new RickeCompactorHandler());
            pipeline.addLast("handler", new RiceServerHandler(tsSupplier));
            return pipeline;
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
                // logger.info(e.toString());
            }
            super.handleUpstream(ctx, e);
        }

        @Override
        public void messageReceived(
                ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            HttpRequest req = (HttpRequest) e.getMessage();
            Map<String, List<String>> parameters = new QueryStringDecoder(req.getUri()).getParameters();
            String endPoint = getEndPoint(req);
            TimeSeries timeSeries = tsSupplier.get();
            if (StringUtils.equalsIgnoreCase("inc", endPoint)) {
                TimerContext timerContext = increments.time();
                try {
                    IncrementParser incrementRequest = new IncrementParser(parameters);

                    timeSeries.inc(incrementRequest.getType(), incrementRequest.getKey(),
                            incrementRequest.getTimeStamp(), incrementRequest.getMetrics());
                    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
                    response.setContent(ChannelBuffers.copiedBuffer("", CharsetUtil.UTF_8));
                    response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");

                    if (isKeepAlive(req)) {
                        // Add 'Content-Length' header only for a keep-alive connection.
                        response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, response.getContent().readableBytes());
                    }
                    // Write the response.
                    ChannelFuture future = e.getChannel().write(response);

                    // Close the non-keep-alive connection after the write operation is done.
                    if (!isKeepAlive(req)) {
                        future.addListener(ChannelFutureListener.CLOSE);
                    }
                } finally {
                    timerContext.stop();
                }
            } else if (StringUtils.equalsIgnoreCase("get", endPoint)) {
                TimerContext timerContext = gets.time();
                GetParser getRequest = new GetParser(parameters);
                try {
                    Collection<DataPoints> dataPoints = timeSeries.get(getRequest.getType(), getRequest.getKey(),
                            new Interval(getRequest.getStartTimeStamp(), getRequest.getEndTimeStamp()),
                            getRequest.getMetrics());
                    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
                    if (StringUtils.isNotBlank(getRequest.getCallback())) {
                        response.setContent(ChannelBuffers.copiedBuffer(
                                getRequest.getCallback() + "(" + gson.toJson(dataPoints)
                                        + ");", CharsetUtil.UTF_8));
                    } else {
                        response.setContent(ChannelBuffers.copiedBuffer(gson.toJson(dataPoints), CharsetUtil.UTF_8));
                    }
                    response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");

                    if (isKeepAlive(req)) {
                        // Add 'Content-Length' header only for a keep-alive connection.
                        response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, response.getContent().readableBytes());
                    }
                    // Write the response.
                    ChannelFuture future = e.getChannel().write(response);

                    // Close the non-keep-alive connection after the write operation is done.
                    if (!isKeepAlive(req)) {
                        future.addListener(ChannelFutureListener.CLOSE);
                    }

                } finally {
                    timerContext.stop();
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

        private String getEndPoint(final HttpRequest req) {
            final String uri = req.getUri();
            if (uri.length() < 1) {
                throw new IllegalArgumentException("Empty query");
            }
            if (uri.charAt(0) != '/') {
                throw new IllegalArgumentException("Query doesn't start with a slash: <code>"
                        // TODO(tsuna): HTML escape to avoid XSS.
                        + uri + "</code>");
            }
            final int questionmark = uri.indexOf('?', 1);
            final int slash = uri.indexOf('/', 1);
            int pos; // Will be set to where the first path segment ends.
            if (questionmark > 0) {
                if (slash > 0) {
                    pos = (questionmark < slash
                            ? questionmark // Request: /foo?bar/quux
                            : slash); // Request: /foo/bar?quux
                } else {
                    pos = questionmark; // Request: /foo?bar
                }
            } else {
                pos = (slash > 0
                        ? slash // Request: /foo/bar
                        : uri.length()); // Request: /foo
            }
            return uri.substring(1, pos);
        }

        private class IncrementParser {

            private Map<String, List<String>> params;

            public IncrementParser(Map<String, List<String>> params) {
                this.params = params;
            }

            String getParameter(String name) {
                List<String> list = params.get(name);
                if (null != list && !list.isEmpty()) {
                    return list.get(0);
                }
                return null;
            }

            String getRequiredParameter(String name) {
                return params.get(name).get(0);
            }

            List<String> getParameterValues(String name) {
                return params.get(name);
            }

            public String getKey() {
                return getRequiredParameter("k");
            }

            public long getTimeStamp() {
                String timestamp = getParameter("ts");
                return null == timestamp ? System.currentTimeMillis() : new Long(timestamp);
            }

            public String getType() {
                return getRequiredParameter("t");
            }

            public Map<String, Long> getMetrics() {
                Map<String, Long> result = new HashMap<String, Long>();
                String metricsJson = getParameter("metrics");
                if (null != metricsJson) {
                    Map<String, Long> metrics = gson.fromJson(metricsJson, mapType);
                    result.putAll(metrics);
                }
                return result;

            }
        }

        private class GetParser {

            private Map<String, List<String>> params;

            public GetParser(Map<String, List<String>> params) {
                this.params = params;
            }

            /**
             * @return
             */
            public String getCallback() {
                return getParameter("_callback");
            }

            String getParameter(String name) {
                List<String> list = params.get(name);
                if (null != list && !list.isEmpty()) {
                    return list.get(0);
                }
                return null;
            }

            String getRequiredParameter(String name) {
                return params.get(name).get(0);
            }

            List<String> getParameterValues(String name) {
                return params.get(name);
            }

            public String getKey() {
                return getRequiredParameter("k");
            }

            public long getStartTimeStamp() {
                String timestamp = getParameter("s");
                return null == timestamp ? System.currentTimeMillis() : new Long(timestamp);
            }

            public long getEndTimeStamp() {
                String timestamp = getParameter("e");
                return null == timestamp ? System.currentTimeMillis() : new Long(timestamp);
            }

            public String getType() {
                return getRequiredParameter("t");
            }

            public List<String> getMetrics() {
                List<String> result = new ArrayList<String>();
                String metricsJson = getParameter("metrics");
                if (null != metricsJson) {
                    List<String> metrics = gson.fromJson(metricsJson, listType);
                    result.addAll(metrics);
                }
                return result;

            }
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
