package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TupleRDDExample {
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

        //JavaRDD<Double> sqrtRDD = myRDD.map(value -> Math.sqrt(value));

        JavaRDD<Tuple2<Integer, Double>> numberSqrtTupleRDD = myRDD
                .map(value -> new Tuple2(value, Math.sqrt(value)));
        //you are not storing in any map , you are using mapping technique to transform one
        // element of RDD to one tuple element

        numberSqrtTupleRDD.foreach(val -> System.out.println(val._1+","+val._2));
        javaSparkContext.close();
    }
}
