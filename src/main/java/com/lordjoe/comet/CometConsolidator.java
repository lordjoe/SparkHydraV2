package com.lordjoe.comet;

import com.lordjoe.distributed.SparkFileSaver;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.systemsbiology.xml.XMLUtilities;
import org.systemsbiology.xtandem.XTandemMain;
import org.systemsbiology.xtandem.hadoop.XTandemHadoopUtilities;
import org.systemsbiology.xtandem.reporting.BiomlReporter;
import scala.Tuple2;

import java.io.Serializable;

/**
 * com.lordjoe.distributed.hydra.scoring.CometConsolidator     \
 * Responsible for writing an output file
 * User: Steve
 * Date: 10/20/2014
 */
public class CometConsolidator implements Serializable {

    private final String header;
    private final String footer;
    private final XTandemMain application;

    public CometConsolidator(final String pHeader, final String pFooter, final XTandemMain pApplication) {
        footer = pFooter;
        application = pApplication;
        header = pHeader;
    }


    public XTandemMain getApplication() {
        return application;
    }

    /**
     * write scores into a file
     *
     * @param scans
     */
    public void writeScores(JavaRDD<String> textOut) {

        textOut = sortByIndex(textOut);
        String outputPath = BiomlReporter.buildDefaultFileName(application);
        Path path = XTandemHadoopUtilities.getRelativePath(outputPath);

//        List<String> headerList = new ArrayList<>();
//        headerList.add(header);
//
//        List<String> footerList = new ArrayList<>();
//        footerList.add(footer);
//
//        JavaSparkContext sparkContext = SparkUtilities.getCurrentContext();
//        JavaRDD<String> headerRdd = sparkContext.parallelize(headerList);
//        JavaRDD<String> footerRdd = sparkContext.parallelize(footerList);
//
//        textOut = headerRdd.union(textOut);
//        textOut = textOut.union(footerRdd);


        SparkFileSaver.saveAsFile(path, textOut,header,footer);
     }


    public static   JavaRDD  sortByIndex(JavaRDD<String> bestScores) {
        JavaPairRDD<String, String> byIndex = bestScores.mapToPair(new TextToScanID());
        JavaPairRDD<String, String> sortedByIndex = byIndex.sortByKey();

        return sortedByIndex.values();
   
    }

     static class TextToScanID   implements PairFunction<String, String, String> {
        @Override
        public Tuple2<String, String> call(final String t) throws Exception {
            String id = XMLUtilities.extractTag("spectrum",t);
            return new Tuple2<String, String>(id, t);
        }
    }


}
