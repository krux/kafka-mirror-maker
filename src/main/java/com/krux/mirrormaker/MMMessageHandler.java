package com.krux.mirrormaker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import com.krux.kafka.consumer.MessageHandler;
import com.krux.kafka.producer.KafkaProducer;

public class MMMessageHandler implements MessageHandler<byte[]> {

    private final KafkaProducer _producer;   
    private final List<byte[]> _topicMessages = Collections.synchronizedList( new ArrayList<byte[]>() );
    private final Object mutex = new Object();
    private int _buffered_messages = 1000;

    public MMMessageHandler( KafkaProducer producer, int buffered_messages ) {
        
        TimerTask publisher = new TimerTask() {
            @Override
            public void run() {
                synchronized ( mutex ) {
                    sendBufferedMessages();
                }
            }
        };
        
        final Timer t = new Timer();
        t.schedule( publisher, 500, 500 );
        
        Runtime.getRuntime().addShutdownHook( new Thread() {
            @Override
            public void run() {
                t.cancel();
            }
        });
        
        _producer = producer;
        _buffered_messages = buffered_messages;
    }

    @Override
    public void onMessage( byte[] message ) {
        synchronized( mutex ) {
            _topicMessages.add( message );
            if ( _topicMessages.size() > _buffered_messages ) {
                sendBufferedMessages();
            }
        }
    }

    private void sendBufferedMessages() {
        if ( _topicMessages.size() > 0 ) {
            for ( byte[] message : _topicMessages ) {
                _producer.send( message );
            }
        }
        _topicMessages.clear();
    }

}
