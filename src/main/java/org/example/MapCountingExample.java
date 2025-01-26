package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class MapCountingExample {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        SparkConf sparkConf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> myRDD = javaSparkContext.parallelize(inputData);

        Integer result = myRDD.reduce((value1, value2) -> value1+value2);

        JavaRDD<Double> sqrtRDD = myRDD.map(value -> Math.sqrt(value));

        System.out.println(result);

        System.out.println(sqrtRDD.count());//count function in RDD
        javaSparkContext.close();
    }
}
