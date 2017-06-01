package com.rswestmoreland.graylog2.plugin;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.DropdownField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.BooleanField;
import org.graylog2.plugin.outputs.MessageOutput;
import org.graylog2.plugin.streams.Stream;
import org.graylog2.plugin.system.NodeId;

import com.google.common.collect.ImmutableMap;
import com.google.inject.assistedinject.Assisted;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;


/**
 * Implementation of plugin to Graylog 2.0+ to send stream via delimited file

 * The SyslogOutput Plugin (by Huksley and TCollins) was used as the initial framework
 * I still have come cleanup, refactoring, and commenting to do.
 * 
 * @author RSWestmoreland, DelimitedFileOutput
 */
public class DelimitedFileOutput implements MessageOutput {

	private static final Logger LOG = Logger.getLogger(DelimitedFileOutput.class.getName());
        public static AtomicBoolean isRunning = new AtomicBoolean(false);
        
        ScheduledExecutorService scheduler = newSingleThreadScheduledExecutor();
        public static String endlineSequence;
        public static String filepath;
        public static String filedone;
        public static String fields;
        public static String[] fieldList;
        public static int fieldListSize = 0;
        public static boolean countLines = false;

        private static final String DEFAULTPATH = "/var/tmp/export.$PID.$EPOCH";
        private static final String DEFAULTDONE = ".csv";
        private static final String DEFAULTFIELDS = "timestamp, source, message";
        private static final int DEFAULTBUFFER = 8192;
        private static final int DEFAULTINTERVAL = 1;
        private static final String DEFAULTENDLINE = "newline";
        private static final int DEFAULTROTATEINT = 3600;
        
        public static String pid;
        public static String host;
        private final NodeId nodeId;
        public static String node;

	public static String fileformat;
        public static String endline;
        public static String compress;
        public static String strategy;
        public static int rotateinterval;
        public static int buffersize;
        public static int flushinterval;
        public static boolean debug = false;

	private static MessageSender sender;
        
         
	public static MessageSender createSender(String format) {
            switch (format) {
                case "csv":
                    return new FormatCSV();
                case "pipe":
                    return new FormatPipe();
                case "space":
                    return new FormatSpace();
                case "tsv":
                    return new FormatTSV();
                default:
                    return new FormatCSV();
            }
	}


	@Inject
	public DelimitedFileOutput(@Assisted Stream stream, @Assisted Configuration config, NodeId nodeId) throws IOException, InterruptedException {
            LOG.info("Initializing Delimited File Output plugin");
            
            pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            host = ManagementFactory.getRuntimeMXBean().getName().split("@")[1];
            this.nodeId = nodeId;
            node = this.nodeId.toString();
 
            filepath = config.getString("filepath");
            if ( filepath == null || filepath.equals("")) {
                filepath = DEFAULTPATH;
            }
            
            filedone = config.getString("filedone");
            if ( filedone == null ) {
                filedone = DEFAULTDONE;
            }
            
            fields = config.getString("fields");
            if ( fields == null || fields.equals("")) {
                fields = DEFAULTFIELDS;
            }
            fields = fields.replaceAll("\\s","");
            fieldList = fields.split(",");
            fieldListSize = (fieldList.length - 1);
            
            fileformat = config.getString("fileformat");
            if ( fileformat == null || fileformat.equals("")) {
            	fileformat = "csv";
            }
            
            endline = config.getString("endline");
            switch (endline) {
                case "newline":
                    endlineSequence = "\n";
                    break;
                case "crlf":
                    endlineSequence = "\r\n";
                    break;
                default:
                    endline = DEFAULTENDLINE;
                    break;
            }
            
            compress = config.getString("compress");
            if ( compress == null || compress.equals("")) {
                compress = "none";
            }
            
            strategy = config.getString("strategy");
            if ( strategy == null || strategy.equals("")) {
                strategy = "interval";
            }
            
            rotateinterval = config.getInt("rotateinterval");
            if ( rotateinterval < 0 ) {
                rotateinterval = DEFAULTROTATEINT;
            }
            
            buffersize = config.getInt("buffersize");
            if ( buffersize <= 0 ) {
                buffersize = DEFAULTBUFFER;
            }
            
            flushinterval = config.getInt("flushinterval");
            if ( flushinterval < 0 ) {
                flushinterval = DEFAULTINTERVAL;
            }
            
            debug = config.getBoolean("debug");
            
            FileWrite.NewFile(filepath);
            
            if ( rotateinterval != 0 ) {
                if ( strategy.equals("interval") ) {
                    scheduler.scheduleAtFixedRate(FileWrite.PeriodicRotate,rotateinterval,rotateinterval,TimeUnit.SECONDS);
                } else {
                    countLines = true;
                }
            }
            if ( flushinterval != 0 ) {
                scheduler.scheduleAtFixedRate(FileWrite.PeriodicFlush,flushinterval,flushinterval,TimeUnit.SECONDS);
            }
            
            sender = createSender(fileformat);
            
            isRunning.set(true);
            LOG.log(Level.INFO, "Delimited File Output Plugin started");
            
	}

        @Override
        public boolean isRunning() {
            return isRunning.get();
        }
        
        @Override
        public void stop() {

            sender = null;
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(flushinterval, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                    scheduler.awaitTermination(flushinterval, TimeUnit.SECONDS);
                }
            } catch (InterruptedException ex) {
                scheduler.shutdownNow();
                LOG.log(Level.SEVERE, null, ex);
                Thread.currentThread().interrupt();
            }
            
            try {
                FileWrite.CloseFile();
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
            isRunning.set(false);
            LOG.log(Level.INFO, "Delimited File Output Plugin stopped");
        }
        
	@Override
	public void write(List<Message> msgs) throws Exception {
            for (Message msg: msgs) {
                write(msg);
            }
	}

	@Override
	public void write(Message msg) throws Exception {
            if (sender != null) {
                sender.send(msg);
            }
	}

	public interface Factory extends MessageOutput.Factory<DelimitedFileOutput> {
            @Override
            DelimitedFileOutput create(Stream stream, Configuration configuration);

            @Override
            Config getConfig();

            @Override
            Descriptor getDescriptor();
	}

	public static class Descriptor extends MessageOutput.Descriptor {
            public Descriptor() {
                super("Delimited File Output", false, "", "Forwards stream to disk in delimited format.");
            }
	}

	public static class Config extends MessageOutput.Config {
            @Override
            public ConfigurationRequest getRequestedConfiguration() {
            	ConfigurationRequest configurationRequest = new ConfigurationRequest();

            	Map<String, String> formats = ImmutableMap.of("csv", "CSV", "tsv", "TSV", "pipe", "Pipe", "space", "Space");
            	configurationRequest.addField(new DropdownField(
            			"fileformat", "File Format", "csv", formats,
            			"The file format that should be used to write messages to disk.",
            			ConfigurationField.Optional.NOT_OPTIONAL)
            	);
                        
            	configurationRequest.addField(new TextField("filepath", "File Path", DEFAULTPATH, 
                        "File path to write messages to.  Available substitution variables are $HOST, $NODE, $EPOCH, $PID, $THREAD, $ROTATE, $PADDED.", ConfigurationField.Optional.NOT_OPTIONAL));
                configurationRequest.addField(new TextField("fields", "Fields", DEFAULTFIELDS, "Comma separated fields to export.", ConfigurationField.Optional.NOT_OPTIONAL, TextField.Attribute.TEXTAREA));

            	Map<String, String> endlines = ImmutableMap.of("newline", "Newline", "crlf", "CRLF");
            	configurationRequest.addField(new DropdownField(
            			"endline", "End of Line", "newline", endlines,
            			"The special characters used by systems to represent end of line.",
            			ConfigurationField.Optional.NOT_OPTIONAL)
            	);

           	Map<String, String> compresslist = ImmutableMap.of("none", "None", "gzip", "GZIP", "gzip_fast", "GZIP Fastest", "gzip_max", "GZIP Max Compression");
            	configurationRequest.addField(new DropdownField(
            			"compress", "Compression Options", "none", compresslist,
            			"Optionally compress the file in realtime.",
            			ConfigurationField.Optional.NOT_OPTIONAL)
            	);
                        
                Map<String, String> strategies = ImmutableMap.of("interval", "Interval", "count", "Count");
		configurationRequest.addField(new DropdownField(
				"strategy", "Rotation Strategy", "interval", strategies,
				"How the output filename will change to prevent unlimited growth.",
				ConfigurationField.Optional.NOT_OPTIONAL)
		);
                        
		configurationRequest.addField(new NumberField("rotateinterval", "Rotate Counter", DEFAULTROTATEINT, "Seconds or line count until file rotation, depending on selected strategy.  Disable by setting to zero.", ConfigurationField.Optional.NOT_OPTIONAL, NumberField.Attribute.ONLY_POSITIVE));
                configurationRequest.addField(new TextField("filedone", "Append Extension", DEFAULTDONE, "Append file with extension when rotating.", ConfigurationField.Optional.OPTIONAL));
                                                
                configurationRequest.addField(new NumberField("buffersize", "Buffer Size", DEFAULTBUFFER, "Write buffer in bytes. Must be greater than zero, multiple of 1024 recommended.", ConfigurationField.Optional.NOT_OPTIONAL, NumberField.Attribute.ONLY_POSITIVE));
                configurationRequest.addField(new NumberField("flushinterval", "Flush Interval", DEFAULTINTERVAL, "Seconds to flush write buffer.  Disable by setting to zero.", ConfigurationField.Optional.NOT_OPTIONAL, NumberField.Attribute.ONLY_POSITIVE));

                configurationRequest.addField(new BooleanField("debug", "Debug", false, "Enable debugging to troubleshoot file writes."));
        
		return configurationRequest;
            }
	}

}
