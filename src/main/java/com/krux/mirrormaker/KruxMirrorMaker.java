package com.krux.mirrormaker;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.consumer.ConsumerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.krux.kafka.consumer.KafkaConsumer;
import com.krux.kafka.helpers.TopicUtils;
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

        OptionSpec<String> consumerConfig = parser.accepts( "consumer-config", "Absolute path to consumer config file." ).withRequiredArg().ofType( String.class  );
        OptionSpec<String> producerConfig = parser.accepts( "producer-config", "Absolute path to producer config file." ).withRequiredArg().ofType( String.class  );
        OptionSpec<String> blackList = parser.accepts( "blacklist", "Comma-separated list of topics to be excluded from mirroring" ).withRequiredArg().ofType( String.class  );
        OptionSpec<String> whiteList = parser.accepts( "whitelist", "Comma-separated list of topics to be included in mirroring" ).withRequiredArg().ofType( String.class  );
        OptionSpec<Integer> queueSize = parser.accepts( "queue-size", "?" ).withRequiredArg().ofType( Integer.class  ).defaultsTo( 1 );

        KruxStdLib.setOptionParser( parser );
        OptionSet options = KruxStdLib.initialize( args );

        // ensure required cl options are present
        if ( !options.has( consumerConfig ) || !options.has( producerConfig ) ) {
            LOG.error( "'--consumer-config' and '--producer-config' are required parameters. Exitting!" );
            System.exit( -1 );
        }
        
        try {
        Properties consumerProperties = new Properties();
        consumerProperties.load(  new FileInputStream( options.valueOf( consumerConfig ) ) );

        Properties producerProperties = new Properties();
        producerProperties.load(  new FileInputStream( options.valueOf( producerConfig ) ) );
        
        //deal with topic list
        
        // if whitelist is specified, use that
        List<String> allTopics = new ArrayList<String>();
        if ( options.has( whiteList ) ) {
            String[] topics = options.valueOf( whiteList ).split( "," );
            for ( String topic : topics ) {
                allTopics.add( topic.trim() );
            }
        } else {
            // get all topics
            String zkUrl = consumerProperties.getProperty( "zookeeper.connect" );
            allTopics = TopicUtils.getAllTopics( zkUrl );
        }
        
        if ( allTopics == null || allTopics.size() == 0 ) {
            LOG.error( "No topics specified for mirroring." );
            System.exit( -1 );
        }
        
        //if blacklist, remove those from topics to be mirrored
        if ( options.has( blackList ) ) {
            String[] topics = options.valueOf( whiteList ).split( "," );
            for ( String topic : topics ) {
                allTopics.remove( topic.trim() );
            }
        }

        StringBuilder sb = new StringBuilder();
        for ( String topic : allTopics ) {
            sb.append( topic );
            sb.append( " " );
        }
        
        LOG.info( "Will mirror: " + sb.toString() );
        
        //one thread per topic for now
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        for ( String topic : allTopics ) {
            topicMap.put(  topic, 1 );
        }
        
        } catch ( Exception e ) {
            LOG.error( "oops", e );
            System.exit( -1 );
        }
        
        
        // parse out topic->thread count mappings
//        List<String> topicThreadMappings = options.valuesOf( topicThreadMapping );
//        Map<String, Integer> topicMap = new HashMap<String, Integer>();
//
//        for ( String topicThreadCount : topicThreadMappings ) {
//            if ( topicThreadCount.contains( "," ) ) {
//                String[] parts = topicThreadCount.split( "," );
//                topicMap.put( parts[0], Integer.parseInt( parts[1] ) );
//            } else {
//                topicMap.put( topicThreadCount, 1 );
//            }
//        }
//
//        // create single ConsumerConfig for all mappings. Topic and thread
//        // counts will be overridden in BeaconStreamLogger
//        ConsumerConfig config = KafkaConsumer.createConsumerConfig( options, optionSpecs );

        // now setup producer(s) for pushing messages to other cluster

        // KafkaConsumer runner = new KafkaConsumer(config, topicMap);
        // runner.run();

    }

}
