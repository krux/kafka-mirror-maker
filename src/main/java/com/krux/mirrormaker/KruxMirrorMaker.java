package com.krux.mirrormaker;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.kafka.consumer.KafkaConsumer;
import com.krux.kafka.helpers.TopicUtils;
import com.krux.kafka.producer.KafkaProducer;
import com.krux.server.http.StdHttpServerHandler;
import com.krux.stdlib.KruxStdLib;

public class KruxMirrorMaker {

    private static final Logger LOG = LoggerFactory.getLogger(KruxMirrorMaker.class);

    public static void main(String[] args) {

        Map<String, OptionSpec> optionSpecs = new HashMap<String, OptionSpec>();

        /**
         * Setting up all consumer-related options (expose all kafka consumer
         * config params to the cli)
         */

        OptionParser parser = new OptionParser();

        OptionSpec<String> consumerConfig = parser.accepts("consumer-config", "Absolute path to consumer config file.")
                .withRequiredArg().ofType(String.class);
        OptionSpec<String> producerConfig = parser.accepts("producer-config", "Absolute path to producer config file.")
                .withRequiredArg().ofType(String.class);
        OptionSpec<String> blackList = parser
                .accepts("blacklist", "Comma-separated list of topics to be excluded from mirroring").withRequiredArg()
                .ofType(String.class);
        OptionSpec<String> whiteList = parser
                .accepts("whitelist", "Comma-separated list of topics to be included in mirroring").withRequiredArg()
                .ofType(String.class);
        OptionSpec<Integer> queueSize = parser
                .accepts("queue-size", "Number of messages buffered between consumer and producer.").withRequiredArg()
                .ofType(Integer.class).defaultsTo(1000);
        OptionSpec<Integer> numConsumerStreams = parser.accepts("num-streams", "Number of consumer threads per topic.")
                .withRequiredArg().ofType(Integer.class).defaultsTo(1);

        KruxStdLib.setOptionParser(parser);
        OptionSet options = KruxStdLib.initialize(args);

        // ensure required cl options are present
        if (!options.has(consumerConfig) || !options.has(producerConfig)) {
            LOG.error("'--consumer-config' and '--producer-config' are required parameters. Exitting!");
            System.exit(1);
        }

        if (options.valueOf(queueSize) < 0) {
            LOG.error("'--queue-size' must be > 0. Exitting.");
            System.exit(1);
        }

        try {
            Properties consumerProperties = new Properties();
            consumerProperties.load(new FileInputStream(options.valueOf(consumerConfig)));

            //tmp hack to work around outdated kafka config
            String hostname = InetAddress.getLocalHost().getHostName();

            String zkConnect = consumerProperties.getProperty("zookeeper.connect");
            if (hostname.contains("dub")) {
                zkConnect = zkConnect.replace("kafka-test", "kafka-a-dub");
            } else if (hostname.contains("pdx")) {
                zkConnect = zkConnect.replace("kafka-test", "kafka-a-pdx");
            }
            LOG.warn("Overriding configured kafka consumer url!! Using " + zkConnect);
            consumerProperties.setProperty("zookeeper.connect", zkConnect);

            Properties producerProperties = new Properties();
            producerProperties.load(new FileInputStream(options.valueOf(producerConfig)));
            producerProperties.put("partitioner.class", "com.krux.kafka.producer.SimplePartitioner");
            int socketBufferSize = 512 * 1024;
            producerProperties.put("send.buffer.bytes", String.valueOf(socketBufferSize));
            producerProperties.put("batch.num.messages", "1000");
            producerProperties.put("compression.codec", "snappy");
            producerProperties.put("producer.type", "async");
            producerProperties.put("request.required.acks", "0");

            // deal with topic list

            // if whitelist is specified, use that
            List<String> allTopics = new ArrayList<String>();
            if (options.has(whiteList)) {
                LOG.info("Whitelist being applied");
                String[] topics = options.valueOf(whiteList).split(",");
                for (String topic : topics) {
                    allTopics.add(topic.trim());
                }
            } else {
                // get all topics
                String zkUrl = consumerProperties.getProperty("zookeeper.connect");
                LOG.info("Fetching all topics from " + zkUrl);
                allTopics = TopicUtils.getAllTopics(zkUrl);
            }

            if (allTopics == null || allTopics.size() == 0) {
                LOG.error("No topics specified for mirroring.");
                System.exit(1);
            }

            // if blacklist, remove those from topics to be mirrored
            if (options.has(blackList)) {
                LOG.info("Purging topics in blacklist");
                String[] topics = options.valueOf(blackList).split(",");
                for (String topic : topics) {
                    allTopics.remove(topic.trim());
                }
            }

            StringBuilder sb = new StringBuilder();
            for (String topic : allTopics) {
                sb.append(topic);
                sb.append(" ");
            }
            LOG.info("Will mirror: " + sb.toString());

            StdHttpServerHandler.addAdditionalStatus("mirrored_topics", allTopics);

            int consumerThreadCount = options.valueOf(numConsumerStreams);
            // one thread per topic for now
            for (String topic : allTopics) {

                LOG.info("Creating consumer/producer pair for topic " + topic + "...");
                Map<String, Integer> topicMap = new HashMap<String, Integer>();
                topicMap.put(topic, consumerThreadCount);

                KafkaProducer producer = new KafkaProducer(producerProperties, topic);
                //MMMessageHandler handler = new MMMessageHandler( producer, options.valueOf( queueSize ) );
                MMMessageHandler handler = new MMMessageHandler(producer, 5000);

                KafkaConsumer consumer = new KafkaConsumer(consumerProperties, topicMap, handler);
                consumer.start();
                LOG.info("...started.");
            }

        } catch (Exception e) {
            LOG.error("oops", e);
            System.exit(1);
        }
    }
}
