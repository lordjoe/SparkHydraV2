package com.lordjoe.comet;

import com.lordjoe.comet.MultiFileExecutor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Option;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

/**
 * com.lordjoe.comet.SparkCatTest
 * User: Steve
 * Date: 11/14/2018
 */
public class SparkCatTest {


    public static final int NUMBER_REPEATS = 10;

    public static List<String> buildItems(String text, int repeats) {
        List<String>ret = new ArrayList<>() ;
        for (int i = 0; i < repeats; i++) {
            ret.add(text + i);

        }
        return ret;
    }


    public static void main(String[] args) throws Exception {


        SparkConf sparkConf = new SparkConf().setAppName("CatTest");

        Option<String> option = sparkConf.getOption("spark.master");

        if (!option.isDefined()) {   // use local over nothing
            sparkConf.setMaster("local[*]");
        }


        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        List<String> start = buildItems("Start ",NUMBER_REPEATS ) ;
        List<String> end = buildItems("End ",NUMBER_REPEATS ) ;

        JavaRDD<String> startRdd = ctx.parallelize(start);
        JavaRDD<String> endRdd = ctx.parallelize(end);

        JavaPairRDD<String, String> cross = startRdd.cartesian(endRdd);

        JavaRDD<Tuple2<String, String>> pairs = cross.map(new Function<Tuple2<String, String>, Tuple2<String, String>>() {

            @Override
            public Tuple2<String, String> call(Tuple2<String, String> x) throws Exception {
                return x;
            }
        });

        JavaRDD<String> map = pairs.map(new Function<Tuple2<String, String>, String>() {

            @Override
            public String call(Tuple2<String, String> x) throws Exception {
                return MultiFileExecutor.concat(x._1,x._2);
            }
        });

        // note the list returned by collect is immutable so we need a copy
        List<String> collect = new ArrayList(map.collect());


        Collections.sort(collect);
        PrintWriter out = new PrintWriter(new FileWriter("Concatinations.txt")) ;
        for (String s : collect) {
            System.out.println(s);
             out.println(s);
        }
        out.close();



    }
}

