package com.lordjoe.comet;

import com.lordjoe.utilities.FileUtilities;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;
import java.util.Date;
import java.util.List;

/**
 * com.lordjoe.comet.JoinAndMaximizeTests
 * User: Steve
 * Date: 11/11/2018
 */
public class JoinAndMaximizeTests {

    public static final int NUMBER_LINES = 10;

    /**
     * make an array of files for testing
     * @param elements
     * @param index
     */
    public static void makeArray(int elements,int index)   {
        File outDir = new File("TestFiles" + index);
        FileUtilities.expungeDirectory(outDir);
        outDir.mkdirs();
        for (int i = 0; i < elements; i++) {
             File f = new File(outDir,"Element" + i + ".txt");
             StringBuilder sb = new StringBuilder();
                for (int j = 0; j < NUMBER_LINES; j++) {
                    sb.append(Integer.toString(j * (int)Math.pow(10,index)) + "\n");

            }
            FileUtilities.writeFile(f,sb.toString());
        }
    }

    public static void getFiles()
    {
        Date start = new Date();
        SparkConf conf = new SparkConf().setAppName("Spark Join")
                .setMaster("local[4]");

        JavaSparkContext context = new JavaSparkContext(conf);

        FileUtilities.expungeDirectory("output");

        JavaPairRDD<String, String> testFiles1 = context.wholeTextFiles("TestFiles1");
        JavaPairRDD<String, String> testFiles2 = context.wholeTextFiles("TestFiles2");

        JavaRDD<String> values1 = testFiles1.values();
        JavaRDD<String> values2 = testFiles2.values();
        JavaPairRDD<String, String> cartesian = values1.cartesian(values2);
        JavaRDD<Tuple2<String, String>> pairs = cartesian.map(new Function<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2;
            }
        });

        List<Tuple2<String, String>> collect = pairs.collect();
        for (Tuple2<String, String> t : collect) {
            System.out.println(t._1);
            System.out.println(t._2);
        }

    }

    public static void makeTestDirectories()
    {
        makeArray(10,1);
        makeArray(20,2);
     }
    public static void main(String[] args) {
        getFiles();
    }

}
