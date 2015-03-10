package org.graylog.outputs.metrics;

import com.google.inject.multibindings.MapBinder;
import org.graylog2.plugin.PluginConfigBean;
import org.graylog2.plugin.PluginModule;
import org.graylog2.plugin.outputs.MessageOutput;

import java.util.Collections;
import java.util.Set;

public class MetricsOutputModule extends PluginModule {
    @Override
    public Set<? extends PluginConfigBean> getConfigBeans() {
        return Collections.emptySet();
    }

    @Override
    protected void configure() {
        final MapBinder<String, MessageOutput.Factory<? extends MessageOutput>> outputMapBinder = outputsMapBinder();
        installOutput(outputMapBinder, MetricsOutput.class, MetricsOutput.Factory.class);
    }
}
