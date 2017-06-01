package com.rswestmoreland.graylog2.plugin;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.ServerStatus.Capability;
import org.graylog2.plugin.Version;

public class DelimitedFileOutputMetaData implements PluginMetaData {

	@Override
	public String getAuthor() {
		return "Richard S. Westmoreland";
	}

	@Override
	public String getDescription() {
		return "Enables sending messages to disk in a delimited format.";
	}

	@Override
	public String getName() {
		return "Delimited File Writer";
	}

	@Override
	public Set<Capability> getRequiredCapabilities() {
		return Collections.emptySet();
	}

	@Override
	public Version getRequiredVersion() {
		return Version.from(2, 1, 1);
	}

	@Override
	public URI getURL() {
		return URI.create("https://github.com/rswestmoreland/graylog2-delimited-file-output-plugin");
	}

	@Override
	public String getUniqueId() {
		return DelimitedFileOutput.class.getName();
	}

	@Override
	public Version getVersion() {
		return new Version(0, 1, 0);
	}
}
