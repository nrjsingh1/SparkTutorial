package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class _2MapReduceExample {
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
        //todo: reduce function , takes two values in the RDD and reduce them
        // to one element in RDD , reducing RDD size by 1 after each operation

        JavaRDD<Double> sqrtRDD = myRDD.map(value -> Math.sqrt(value));
        //you are not storing in any map , you are using mapping technique to transform one
        // element of RDD to its sqrt in another RDD

        System.out.println(result);

        sqrtRDD
                //.foreach(value -> System.out.println(value));
        //function should be serialised and then passed to RDD , as spark internally work with multiple nodes and
        //and just like data is distributed , function also needs to be serialised//
                .collect().forEach(System.out::println);

                //.foreach(System.out::println);
        //if multiple CPUs spark will give NonSerialise error
        //todo: using forEach with RDD not good for performance.

        javaSparkContext.close();
    }
}
