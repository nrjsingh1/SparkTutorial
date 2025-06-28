package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class _1Main {
    //private static Logger logger = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) {
        System.out.println("Hello world!");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(12.49943);
        inputData.add(90.32);
        inputData.add(20.32);

        SparkConf sparkConf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Double> myRDD = javaSparkContext.parallelize(inputData);

        Double result = myRDD
                //.map(value-> 1.0)
                .reduce((value1, value2) -> value1+value2);
        System.out.println(result);
        javaSparkContext.close();



    }
}