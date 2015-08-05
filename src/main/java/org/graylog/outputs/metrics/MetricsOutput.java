package org.graylog.outputs.metrics;

import com.codahale.metrics.*;
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
    private List<String> metricFields;

    @Inject
    public MetricsOutput(@Assisted Stream stream, @Assisted Configuration configuration) throws MessageOutputConfigurationException {
        this.configuration = configuration;
        this.metricFields = Arrays.asList(configuration.getString(CK_FIELDS).split(","));

        metricBuffer.clear();

        if (!checkConfiguration(configuration)) {
            throw new RuntimeException("Missing configuration parameters.");
        }

        URI uri = parseUrl(configuration.getString(CK_URL));

        switch (uri.getScheme()) {
            case "graphite":
                graphiteReporter = createGraphiteReporter(uri);
                break;
            case "ganglia":
                gangliaReporter = createGangliaReporter(uri);
                break;
            default:
                LOG.error("Metrics backend not supported!");
                break;
        }

        isRunning.set(true);
    }

    @Override
    public void write(Message message) throws Exception {
        SortedSet<String> currentMetrics = registry.getNames();
        final List<String> validTypes = Arrays.asList("gauge", "counter", "histogram", "meter");

        for (String field : metricFields) {
            field = field.trim();
            String fieldType = "gauge";
            if(field.contains(":")) {
                String[] tupel = field.split(":");
                String type = tupel[tupel.length-1];
                if(validTypes.contains(type)) {
                    fieldType = type;
                    field = tupel[0];
                }
            }

            LOG.trace("Trying to read field [{}] from message <{}>.", field, message.getId());
            if (!message.getFields().containsKey(field)) {
                LOG.debug("Message <{}> does not contain field [{}]. Not sending to metrics store.", message.getId(), field);
            }

            // Get value
            Object messageValue = message.getField(field);
            Number metricValue;
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

            final String metricName = configuration.getBoolean(CK_INCLUDE_SOURCE) ? (message.getSource() + "." + field) : field;
            switch (fieldType.toLowerCase()) {
                case "gauge":
                    // Register metric
                    if (!currentMetrics.contains(metricName)) {
                        registry.register(metricName, new Gauge<Number>() {
                            @Override
                            public Number getValue() {
                                return metricBuffer.get(metricName);
                            }
                        });
                    }
                    // Update metric
                    metricBuffer.put(metricName, metricValue.longValue());
                    break;
                case "counter":
                    final Counter counter = registry.counter(metricName);
                    counter.inc();
                    break;
                case "histogram":
                    final Histogram histogram = registry.histogram(metricName);
                    histogram.update(metricValue.longValue());
                    break;
                case "meter":
                    final Meter meter = registry.meter(metricName);
                    meter.mark(metricValue.longValue());
                    break;
                default:
                    LOG.error("Unknown metric field type for [{}]: {}", metricName, fieldType);
            }

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
                            "A comma separated list of field values in messages that should be transmitted as gauge values." +
                            "Types like counter, meter, histogram can be set like: cache_hit:counter",
                            ConfigurationField.Optional.NOT_OPTIONAL)
            );

            configurationRequest.addField(new NumberField(
                            CK_RUN_RATE,
                            "Submission frequency (seconds)",
                            30,
                            "The period (in seconds) at which Graylog will submit metrics to the endpoint. " +
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
