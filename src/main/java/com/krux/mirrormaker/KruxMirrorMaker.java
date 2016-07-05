package com.krux.mirrormaker;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.PatternSyntaxException;
import java.util.regex.Pattern;

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

    private static final Logger LOG = LoggerFactory.getLogger( KruxMirrorMaker.class );



    public static void main( String[] args ) {

        Map<String, OptionSpec> optionSpecs = new HashMap<String, OptionSpec>();

        /**
         * Setting up all consumer-related options (expose all kafka consumer
         * config params to the cli)
         */

        OptionParser parser = new OptionParser();

        OptionSpec<String> consumerConfig = parser.accepts( "consumer-config", "Absolute path to consumer config file." )
                .withRequiredArg().ofType( String.class );
        OptionSpec<String> producerConfig = parser.accepts( "producer-config", "Absolute path to producer config file." )
                .withRequiredArg().ofType( String.class );
        OptionSpec<String> blackList = parser
                .accepts( "blacklist", "Comma-separated list of topics to be excluded from mirroring or use --regex-filter flag to filter topics with a regular expression." ).withRequiredArg()
                .ofType( String.class );
        OptionSpec<String> whiteList = parser
                .accepts( "whitelist", "Comma-separated list of topics to be included in mirroring or use --regex-filter flag to filter topics with a regular expression." ).withRequiredArg()
                .ofType( String.class );
        OptionSpec<Boolean> RegExpFlag = parser
                .accepts( "regex-filter", "Use regular expression topic filter" ).withOptionalArg()
                .ofType( Boolean.class ).defaultsTo( false );
        OptionSpec<Integer> queueSize = parser
                .accepts( "queue-size", "Number of messages buffered between consumer and producer." ).withRequiredArg()
                .ofType( Integer.class ).defaultsTo( 1000 );
        OptionSpec<Integer> numConsumerStreams = parser.accepts( "num-streams", "Number of consumer threads per topic." )
                .withRequiredArg().ofType( Integer.class ).defaultsTo( 1 );

        KruxStdLib.setOptionParser( parser );
        OptionSet options = KruxStdLib.initialize( args );

        Boolean isRegExpFilter = options.valueOf(RegExpFlag);

        // ensure required cl options are present
        if ( !options.has( consumerConfig ) || !options.has( producerConfig ) ) {
            LOG.error( "'--consumer-config' and '--producer-config' are required parameters. Exiting!" );
            System.exit( 1 );
        }
        
        if ( options.valueOf( queueSize ) < 0 ) {
            LOG.error( "'--queue-size' must be > 0. Exiting." );
            System.exit( 1 );
        }

        if ( options.has( whiteList ) || options.has( blackList ) ) {
            LOG.error( "please use either '--blacklist' or  '--whitelist'" );
            System.exit( 1 );
        }

        try {
            Properties consumerProperties = new Properties();
            consumerProperties.load( new FileInputStream( options.valueOf( consumerConfig ) ) );

            Properties producerProperties = new Properties();
            producerProperties.load( new FileInputStream( options.valueOf( producerConfig ) ) );
            producerProperties.put("partitioner.class", "com.krux.kafka.producer.SimplePartitioner" );
            int socketBufferSize = 512*1024;
            producerProperties.put("send.buffer.bytes", String.valueOf( socketBufferSize ));
            producerProperties.put("batch.num.messages", "1000");
            producerProperties.put("compression.codec", "snappy");
            producerProperties.put("producer.type", "async");
            producerProperties.put("request.required.acks", "0");

            // deal with topic list

            // if whitelist is specified, use that
            List<String> allTopics = new ArrayList<String>();

            if ( options.has( whiteList )) {

                LOG.info( "Whitelist being applied" );

                // get whitelist parameter value
                String whiteListVal = options.valueOf( whiteList );

                // build topic list with regex if --regex-filter flag is present
                if (isRegExpFilter) {

                    // get all topics
                    String zkUrl = consumerProperties.getProperty( "zookeeper.connect" );
                    LOG.info( "Fetching all topics from " + zkUrl );
                    List<String> zkTopics = TopicUtils.getAllTopics( zkUrl );

                    // build regex
                    String regExp = whiteListVal.trim()
                            .replace(',', '|')
                            .replace(" ", "");

                    // filter topic list with whitelist regex
                    for (String topic: zkTopics) {
                        if ( topic.matches(regExp) ) {
                            allTopics.add(topic);
                        }
                    }

                } else {

                    String[] topics = whiteListVal.split(",");
                    for (String topic : topics) {
                        allTopics.add(topic.trim());
                    }

                }
            } else if ( options.has( blackList ) ) {

                // get blacklist parameter value
                String blackListVal = options.valueOf( blackList );

                // get all topics
                String zkUrl = consumerProperties.getProperty( "zookeeper.connect" );
                LOG.info( "Fetching all topics from " + zkUrl );
                allTopics = TopicUtils.getAllTopics( zkUrl );

                // build topic list with regex if --regex-filter flag is present
                if (isRegExpFilter) {
                    // build regex
                    String regExp = blackListVal.trim()
                            .replace(',', '|')
                            .replace(" ", "");

                    // filter topic list with blacklist regex
                    for (String topic: allTopics) {
                        if ( topic.matches(regExp) ) {
                            allTopics.remove(topic.trim());
                        }
                    }

                } else {

                    LOG.info( "Purging topics in blacklist" );
                    String[] topics = blackListVal.split( "," );
                    for ( String topic : topics ) {
                        allTopics.remove(topic.trim());
                    }
                }


            } else {
                LOG.error( "Please specify either a whitelist or blacklist" );
                System.exit( 1 );
            }

            // ensure the topic list is not empty
            if ( allTopics == null || allTopics.size() == 0 ) {
                LOG.error( "No topics specified for mirroring." );
                System.exit( 1 );
            }




            StringBuilder sb = new StringBuilder();
            for ( String topic : allTopics ) {
                sb.append( topic );
                sb.append( " " );
            }
            LOG.info( "Will mirror: " + sb.toString() );
            
            StdHttpServerHandler.addAdditionalStatus("mirrored_topics", allTopics);

            int consumerThreadCount = options.valueOf( numConsumerStreams );
            // one thread per topic for now
            for ( String topic : allTopics ) {

                LOG.info( "Creating consumer/producer pair for topic " + topic + "..." );
                Map<String, Integer> topicMap = new HashMap<String, Integer>();
                topicMap.put( topic, consumerThreadCount );

                KafkaProducer producer = new KafkaProducer( producerProperties, topic );
                //MMMessageHandler handler = new MMMessageHandler( producer, options.valueOf( queueSize ) );
                MMMessageHandler handler = new MMMessageHandler( producer, 5000 );

                KafkaConsumer consumer = new KafkaConsumer( consumerProperties, topicMap, handler );
                consumer.start();
                LOG.info( "...started." );
            }

        } catch ( Exception e ) {
            LOG.error( "oops", e );
            System.exit( 1 );
        }
    }
}
