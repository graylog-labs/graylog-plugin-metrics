# Graylog Metrics Output Plugin

An output plugin for integrating [Graphite](http://graphite.readthedocs.org) and [Ganglia](http://ganglia.info) with [Graylog](https://www.graylog.org).

Please refer to http://docs.graylog.org/en/latest/pages/plugins.html for documentation on how to write
plugins for Graylog.


Getting started
---------------

This project is using Maven 3 and requires Java 7 or higher. The plugin will require Graylog 1.0.0 or higher.

* Clone this repository.
* Run `mvn package` to build a JAR file.
* Optional: Run `mvn jdeb:jdeb` and `mvn rpm:rpm` to create a DEB and RPM package respectively.
* Copy generated JAR file in target directory to your Graylog plugin directory.
* Restart the Graylog.
