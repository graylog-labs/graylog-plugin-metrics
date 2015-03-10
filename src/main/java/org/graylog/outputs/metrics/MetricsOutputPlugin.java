package org.graylog.outputs.metrics;

import org.graylog2.plugin.Plugin;
import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.PluginModule;

import java.util.Arrays;
import java.util.Collection;

public class MetricsOutputPlugin implements Plugin {
    @Override
    public PluginMetaData metadata() {
        return new MetricsOutputMetaData();
    }

    @Override
    public Collection<PluginModule> modules () {
        return Arrays.<PluginModule>asList(new MetricsOutputModule());
    }
}
