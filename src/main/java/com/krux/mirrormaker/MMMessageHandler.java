package com.krux.mirrormaker;

import com.krux.kafka.consumer.MessageHandler;
import com.krux.kafka.producer.KafkaProducer;

public class MMMessageHandler implements MessageHandler<byte[]> {
    
    private final KafkaProducer _producer;

    public MMMessageHandler( KafkaProducer producer ) {
        _producer = producer;
    }

    @Override
    public void onMessage( byte[] message ) {
        _producer.send( message );
    }

}
