package org.graylog.outputs.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.util.concurrent.AtomicLongMap;
import com.google.inject.assistedinject.Assisted;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.BooleanField;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.outputs.MessageOutput;
import org.graylog2.plugin.outputs.MessageOutputConfigurationException;
import org.graylog2.plugin.streams.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MetricsOutput implements MessageOutput {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsOutput.class);

    public static final String CK_URL = "url";
    public static final String CK_PREFIX = "prefix";
    public static final String CK_RUN_RATE = "run_rate";
    public static final String CK_FIELDS = "fields";
    public static final String CK_INCLUDE_SOURCE = "include_source";

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private Configuration configuration;

    private GangliaReporter gangliaReporter;
    private GraphiteReporter graphiteReporter;

    private static final MetricRegistry registry = new MetricRegistry();
    private AtomicLongMap<String> metricBuffer = AtomicLongMap.create();
    private List<String> gaugeFields;

    @Inject
    public MetricsOutput(@Assisted Stream stream, @Assisted Configuration configuration) throws MessageOutputConfigurationException {
        this.configuration = configuration;
        this.gaugeFields = Arrays.asList(configuration.getString(CK_FIELDS).split(","));

        metricBuffer.clear();

        if (!checkConfiguration(configuration)) {
            throw new RuntimeException("Missing configuration parameters.");
        }

        URI uri = parseUrl(configuration.getString(CK_URL));

        if (uri.getScheme().equals("graphite")) {
            graphiteReporter = createGraphiteReporter(uri);
        } else if (uri.getScheme().equals("ganglia")) {
            gangliaReporter = createGangliaReporter(uri);
        } else {
            LOG.error("Metrics backend not supported!");
        }

        isRunning.set(true);
    }

    @Override
    public void write(Message message) throws Exception {
        SortedSet<String> currentMetrics = registry.getNames();

        for (String field : gaugeFields) {
            field = field.trim();

            LOG.trace("Trying to read field [{}] from message <{}>.", field, message.getId());
            if (!message.getFields().containsKey(field)) {
                LOG.debug("Message <{}> does not contain field [{}]. Not sending to metrics store.", message.getId(), field);
            }

            // Get value
            Object messageValue = message.getField(field);
            Number metricValue = 0;
            if (messageValue instanceof Long) {
                metricValue = (Long) messageValue;
            } else if (messageValue instanceof Integer) {
                metricValue = (Integer) messageValue;
            } else if (messageValue instanceof Float) {
                metricValue = (Float) messageValue;
            } else if (messageValue instanceof Double) {
                metricValue = (Double) messageValue;
            } else {
                LOG.debug("Field [{}] of message <{}> is not of numeric type. Not sending to metrics store.",
                        field, message.getId());
                continue;
            }

            // Register metric
            final String gauge_field = configuration.getBoolean(CK_INCLUDE_SOURCE) ? (message.getSource() + "." + field) : field;
            if (!currentMetrics.contains(gauge_field)) {
                registry.register(gauge_field, new Gauge<Number>() {
                    @Override
                    public Number getValue() {
                        return metricBuffer.get(gauge_field);
                    }
                });
            }

            // Update metric
            metricBuffer.put(gauge_field, metricValue.longValue());
        }
    }

    @Override
    public void write(List<Message> messages) throws Exception {
        for (Message message : messages) {
            write(message);
        }
    }

    @Override
    public boolean isRunning() {
        return isRunning.get();
    }

    @Override
    public void stop() {
        LOG.info("Stopping Metrics output");

        registry.removeMatching(MetricFilter.ALL);
        metricBuffer.clear();

        if (gangliaReporter != null) {
            gangliaReporter.close();
        }
        if (graphiteReporter != null) {
            graphiteReporter.close();
        }

        isRunning.set(false);
    }

    public interface Factory extends MessageOutput.Factory<MetricsOutput> {
        @Override
        MetricsOutput create(Stream stream, Configuration configuration);
        @Override
        Config getConfig();
        @Override
        Descriptor getDescriptor();
    }

    public static class Config extends MessageOutput.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            final ConfigurationRequest configurationRequest = new ConfigurationRequest();

            configurationRequest.addField(new TextField(
                            CK_URL,
                            "URL of metrics endpoint",
                            "graphite://localhost:2003",
                            "URL of your Graphite/Ganglia/InfluxDB instance",
                            ConfigurationField.Optional.NOT_OPTIONAL)
            );

            configurationRequest.addField(new TextField(
                            CK_PREFIX,
                            "Prefix for metric names",
                            "org.graylog",
                            "Name of metric will be 'prefix + field name'.",
                            ConfigurationField.Optional.OPTIONAL)
            );

            configurationRequest.addField(new TextField(
                            CK_FIELDS,
                            "Message fields to submit to metrics store",
                            "response_time,db_time,view_time",
                            "A comma separated list of field values in messages that should be transmitted as gauge values.",
                            ConfigurationField.Optional.NOT_OPTIONAL)
            );

            configurationRequest.addField(new NumberField(
                            CK_RUN_RATE,
                            "Submission frequency (seconds)",
                            30,
                            "How often per second will Graylog submit metrics to the endpoint. " +
                            "Keep this number high to not flood the metrics store.",
                            ConfigurationField.Optional.NOT_OPTIONAL,
                            NumberField.Attribute.ONLY_POSITIVE)
            );

            configurationRequest.addField(new BooleanField(
                            CK_INCLUDE_SOURCE,
                            "Include message source in metric name",
                            false,
                            "Metric name will be 'prefix + message source + field name'.")
            );

            return configurationRequest;
        }
    }

    private boolean checkConfiguration(Configuration config) {
        return config.stringIsSet(CK_URL)
                && config.stringIsSet(CK_FIELDS)
                && config.intIsSet(CK_RUN_RATE)
                && config.getInt(CK_RUN_RATE) > 0;
    }

    public static class Descriptor extends MessageOutput.Descriptor {
        public Descriptor() {
            super("Metrics Output", false, "", "Forwards selected field values of your messages to Graphite/Ganglia/InfluxDB.");
        }
    }

    private URI parseUrl(String url) {
        URI uri = null;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            LOG.error("Malformed metrics URL.");
        }
        return uri;
    }

    private GraphiteReporter createGraphiteReporter(URI uri) {
        final Graphite graphite = new Graphite(new InetSocketAddress(uri.getHost(), uri.getPort()));
        GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(registry)
                .prefixedWith(configuration.getString(CK_PREFIX))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
        graphiteReporter.start(configuration.getInt(CK_RUN_RATE), TimeUnit.SECONDS);
        return graphiteReporter;
    }

    private GangliaReporter createGangliaReporter(URI uri) {
        GangliaReporter gangliaReporter = null;
        try {
            final GMetric ganglia = new GMetric(uri.getHost(), uri.getPort(), GMetric.UDPAddressingMode.MULTICAST, 1);
            gangliaReporter = GangliaReporter.forRegistry(registry)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .prefixedWith(configuration.getString(CK_PREFIX))
                    .build(ganglia);
            gangliaReporter.start(configuration.getInt(CK_RUN_RATE), TimeUnit.SECONDS);
        } catch (IOException e) {
            LOG.error("Can not connect to Ganglia server");
        }
        return gangliaReporter;
    }
}
