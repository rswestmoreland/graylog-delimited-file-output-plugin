package com.rswestmoreland.graylog2.plugin;

import org.graylog2.plugin.Message;

/**
 * Optimized sender
 */
public interface MessageSender {
    void send(Message msg);
}