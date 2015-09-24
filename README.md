Krux Mirror Maker
=================

The Krux Mirror Maker is a custom solution for easily providing message replication capabilities between [Kafka](http://kafka.apache.com) broker clusters. It uses the same configuration files as the [Mirror Maker tool provided directly by Kafka](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27846330), but provides two additional capabilities: [Krux Standard Library](https://github.com/krux/java-stdlib)-based configuration, stats and HTTP-status endpoints, and the ability to be configured to send messages over a pre-configured set of SSH tunnels to a remote Kafka cluster.

Sample Usage
------------

The Krux Mirror Maker is deployed and executed as a stand-alone jar from the command line.
```
java -Xms512M -Xmx512M -jar krux-kafka-mirror-maker-0.4.1.jar \
    --consumer-config /usr/local/kkafka/etc/mirror.consumer.properties \
    --producer-config /usr/local/kkafka/etc/mirror.producer.properties \
    --blacklist krux.listener.heartbeat --stats --stats-environment prod --env prod --http-port 9090 \
    --queue-size 1 --log-level INFO
```

The properties files conform to the same syntax as Kafka's Mirror Maker.