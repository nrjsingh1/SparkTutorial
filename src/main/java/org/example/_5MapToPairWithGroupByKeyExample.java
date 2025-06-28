package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class _5MapToPairWithGroupByKeyExample {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");

        SparkConf sparkConf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> myRDD = javaSparkContext.parallelize(inputData);

        JavaPairRDD<String, String> pairRDD = myRDD.mapToPair(str -> {
            String[] col = str.split(":");
            String level = col[0];
            String date = col[1];
            return new Tuple2<>(level, date);
        });
        //todo:JavaPairRDD is given by Tuple2, with return type <String, String>
        // and not <Tuple<String, String>>
        pairRDD.collect().forEach(pair -> System.out.println(pair._1 + "," + pair._2));

        //todo:above log can be grouped in such a way for same level all dates are combined
        //todo: GroupByKey can lead to performance issues

        JavaPairRDD<String, Long> sumsPairRDD = myRDD.mapToPair(str -> {
            String[] col = str.split(":");
            String level = col[0];
            //String date = col[1];
            return new Tuple2<>(level, 1L);
        });
        JavaPairRDD<String, Long> sumsRDD = sumsPairRDD.reduceByKey((val1, val2) -> val1 + val2);
        //todo:we are summing two rows of PairRDD and grouping(reducing) as well based on key

        sumsRDD.collect().forEach(pair -> System.out.println(pair._1 + " has " + pair._2 + " instances"));

        //todo:FluentApi
        javaSparkContext
                .parallelize(inputData)
                .mapToPair(str -> new Tuple2<>(str.split(":")[0], 1L))
                .reduceByKey((val1, val2) -> val1 + val2)
                .foreach(pair -> System.out.println(pair._1 + " has " + pair._2 + " instances"));

        javaSparkContext.close();
    }
}
