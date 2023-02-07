package com.tutorial.spark;

import javafx.util.Duration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SpringStreaming {
    public static void main(String[] args) throws Exception {
        try{

//            SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate();


            SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
                    ;
            JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

            JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
            lines.print();
            jssc.start();
            jssc.awaitTermination();
        }catch(Exception e){
            System.out.println(e.getMessage());
        }

    }
}
