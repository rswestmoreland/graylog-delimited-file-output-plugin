package com.rswestmoreland.graylog2.plugin;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.graylog2.plugin.Message;

/**
 * Formats fields into message text 
 *     	  						   						 	  	    
 */
public class FormatTSV implements MessageSender {
	private static final Logger LOG = Logger.getLogger(FormatTSV.class.getName());
	
        @Override
	public void send(Message msg) {

            if (DelimitedFileOutput.debug == true) {
                try {
                    FileWrite.DebugFile(msg.getFieldNames().toString(), Long.toString(Thread.currentThread().getId()));
                } catch (IOException ex) {
                    LOG.log(Level.SEVERE, null, ex);
                }
            }
            
            StringBuilder out = new StringBuilder();

            for (int field = 0; field < DelimitedFileOutput.fieldListSize; field++) {
                out.append(msg.hasField(DelimitedFileOutput.fieldList[field]) == true ? 
                        msg.getField(DelimitedFileOutput.fieldList[field]).toString() : "\\N").append("\t");
            }
            out.append(msg.hasField(DelimitedFileOutput.fieldList[DelimitedFileOutput.fieldListSize]) == true ? 
                    msg.getField(DelimitedFileOutput.fieldList[DelimitedFileOutput.fieldListSize]).toString() : "\\N");
                
            try {
                FileWrite.Singleton.INSTANCE.WriteFile(out);
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
	}

}
