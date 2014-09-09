package com.krux.mirrormaker;

import com.krux.kafka.consumer.MessageHandler;
import com.krux.kafka.producer.KafkaProducer;

public class MMMessageHandler implements MessageHandler<byte[]> {
    
    private final String _topic;

    public MMMessageHandler( String topic ) {
        _topic = topic;
    }
    
    @Override
    public void onMessage( byte[] message ) {
        //send message to 
//        KafkaProducer producer = new KafkaProducer();
//        producer.send( new String( message ) );
        
    }

}
