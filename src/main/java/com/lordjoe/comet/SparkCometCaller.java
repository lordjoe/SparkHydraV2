package com.lordjoe.comet;

import com.lordjoe.algorithms.Long_Formatter;
import com.lordjoe.algorithms.MapOfLists;
import com.lordjoe.distributed.PercentileFilter;
import com.lordjoe.distributed.SparkUtilities;
import com.lordjoe.distributed.hydra.comet.CometScoredScan;
import com.lordjoe.distributed.hydra.comet.CometScoringAlgorithm;
import com.lordjoe.distributed.hydra.comet_spark.CometScoringHandler;
import com.lordjoe.distributed.hydra.fragment.BinChargeKey;
import com.lordjoe.distributed.hydra.scoring.SparkMapReduceScoringHandler;
import com.lordjoe.distributed.input.MultiMZXMLScanInputFormat;
import com.lordjoe.distributed.spark.IdentityFunction;
import com.lordjoe.distributed.spark.accumulators.AbstractLoggingFunction;
import com.lordjoe.distributed.spark.accumulators.AccumulatorUtilities;
import com.lordjoe.distributed.spectrum.SparkSpectrumUtilities;
import com.lordjoe.distributed.test.CometSpectraUse;
import com.lordjoe.utilities.ElapsedTimer;
import com.lordjoe.utilities.FileUtilities;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.systemsbiology.xtandem.IMeasuredSpectrum;
import org.systemsbiology.xtandem.XTandemMain;
import org.systemsbiology.xtandem.hadoop.XTandemHadoopUtilities;
import org.systemsbiology.xtandem.ionization.ITheoreticalSpectrumSet;
import org.systemsbiology.xtandem.peptide.IPolypeptide;
import org.systemsbiology.xtandem.pepxml.PepXMLWriter;
import org.systemsbiology.xtandem.scoring.IScoredScan;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * com.lordjoe.comet.SparkCometCaller
 * uses calls to comet directly
 * Date: 10/7/2014
 */
public class SparkCometCaller  implements Serializable {

    public static final boolean DO_DEBUGGING_COUNT = true;

    private static boolean debuggingCountMade = DO_DEBUGGING_COUNT;

    public static boolean isDebuggingCountMade() {
        return debuggingCountMade;
    }

    public static void setDebuggingCountMade(final boolean pIsDebuggingCountMade) {
        debuggingCountMade = pIsDebuggingCountMade;
    }

    private static int maxBinSpectra = 30; // todo make this configurable

    public static int getMaxBinSpectra() {
        return maxBinSpectra;
    }

    public static void setMaxBinSpectra(int maxBinSpectra) {
        SparkCometCaller.maxBinSpectra = maxBinSpectra;
    }

    public static final int SPARK_CONFIG_INDEX = 0;
    public static final int TANDEM_CONFIG_INDEX = 1;
    public static final int SPECTRA_INDEX = 2;
    public static final int SPECTRA_TO_SCORE = Integer.MAX_VALUE;
    public static final String MAX_PROTEINS_PROPERTY = "com.lordjoe.distributed.hydra.MaxProteins";
    @SuppressWarnings("UnusedDeclaration")
    public static final String MAX_SPECTRA_PROPERTY = "com.lordjoe.distributed.hydra.MaxSpectra";
    @SuppressWarnings("UnusedDeclaration")
    public static final String SKIP_SCORING_PROPERTY = "com.lordjoe.distributed.hydra.SkipScoring";
    public static final String SCORING_PARTITIONS_SCANS_NAME = "com.lordjoe.distributed.max_scoring_partition_scans";
    public static final long MAX_SPECTRA_TO_SCORE_IN_ONE_PASS = Long.MAX_VALUE;




    public static class PairCounter implements Comparable<PairCounter> {
        public final BinChargeKey key;
        public final long v1;
        public final long v2;
        public final long product;

        public PairCounter(BinChargeKey pkey, final long pV1, final long pV2) {
            v1 = pV1;
            v2 = pV2;
            key = pkey;
            product = v1 * v2;
        }

        @Override
        public int compareTo(final PairCounter o) {
            return Long.compare(o.product, product);
        }

        public String toString() {
            return key.toString() + "spectra " + Long_Formatter.format(v1) + " peptides " + Long_Formatter.format(v2) +
                    " product " + Long_Formatter.format(product);

        }
    }

    public static <T extends IMeasuredSpectrum> List<PairCounter> showBinPairSizes(final JavaPairRDD<BinChargeKey, ITheoreticalSpectrumSet> keyedPeptides,
                                                                                   final JavaPairRDD<BinChargeKey, T> keyedSpectra) {
        // Map spectra into bins
        Map<BinChargeKey, Long> spectraCountsMap = keyedSpectra.countByKey();
        Map<BinChargeKey, Long> peptideCounts = keyedPeptides.countByKey();
        List<BinChargeKey> keys = new ArrayList(peptideCounts.keySet());
        List<PairCounter> pairs = new ArrayList<PairCounter>();

        long specCount = 0;
        long peptideCount = 0;
        long pairCount = 0;

        Collections.sort(keys);
        for (BinChargeKey key : keys) {
            Object spectralCount = spectraCountsMap.get(key);
            Object peptideCountObj = peptideCounts.get(key);
            if (spectralCount == null || peptideCountObj == null)
                continue;
            long spCount = Long.parseLong(spectralCount.toString());
            specCount += spCount;
            long pepCount = Long.parseLong(peptideCountObj.toString());
            peptideCount += pepCount;
            PairCounter pairCounter = new PairCounter(key, spCount, pepCount);
            pairs.add(pairCounter);
            pairCount += pairCounter.product;
        }

        Collections.sort(pairs);
        List<PairCounter> pairCounters = pairs.subList(0, Math.min(200, pairs.size()));
        for (PairCounter pairCounter : pairCounters) {
            System.err.println(pairCounter.toString());
        }

        System.err.println("Total Spectra " + Long_Formatter.format(specCount) +
                " peptides " + Long_Formatter.format(peptideCount) +
                " bins " + keys.size() +
                " pairs " + Long_Formatter.format(pairCount)

        );
        return pairs;
    }


    public static SparkMapReduceScoringHandler buildCometScoringHandler(String arg) {
        Properties sparkPropertiesX = SparkUtilities.getSparkProperties();

        String pathPrepend = sparkPropertiesX.getProperty(SparkUtilities.PATH_PREPEND_PROPERTY);
        if (pathPrepend != null)
            XTandemHadoopUtilities.setDefaultPath(pathPrepend);

        String maxScoringPartitionSize = sparkPropertiesX.getProperty(SCORING_PARTITIONS_SCANS_NAME);
        if (maxScoringPartitionSize != null)
            SparkMapReduceScoringHandler.setMaxScoringPartitionSize(Integer.parseInt(maxScoringPartitionSize));


        String configStr = SparkUtilities.buildPath(arg);

        //Configuration hadoopConfiguration = SparkUtilities.getHadoopConfiguration();
        //hadoopConfiguration.setLong(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, 64 * 1024L * 1024L);

        //Configuration hadoopConfiguration2 = SparkUtilities.getHadoopConfiguration();  // did we change the original or a copy
        return new SparkMapReduceScoringHandler(configStr, false);
    }

    public static void buildDesiredScoring(final String[] pArgs) {
        if (pArgs.length > TANDEM_CONFIG_INDEX + 1) {
            String fileName = pArgs[TANDEM_CONFIG_INDEX + 1];
            File file = new File(fileName);
            CometSpectraUse desired = new CometSpectraUse(file);
            SparkUtilities.setDesiredUse(desired);
        }
        // shut up the most obnoxious logging
        SparkUtilities.setLogToWarn();

    }



    /**
     * score with a join of a List of peptides
     *
     * @param args
     */
    public static void scoringUsingCogroup(String[] args) {
//        Map<Integer, RawPeptideScan> mapped = CometTestingUtilities.getScanMapFromResource("/eg3_20/eg3_20.mzXML");
//        RawPeptideScan scan2 = mapped.get(2);
//
//


        long totalSpectra = 0;
        List<PairCounter> pairs = null;

        // Force PepXMLWriter to load
        PepXMLWriter foo = null;
        ElapsedTimer timer = new ElapsedTimer();
        ElapsedTimer totalTime = new ElapsedTimer();

        if (args.length < TANDEM_CONFIG_INDEX + 1) {
            System.out.println("usage sparkconfig configFile");
            return;
        }

        buildDesiredScoring(args);

        SparkUtilities.readSparkProperties(args[SPARK_CONFIG_INDEX]);

        SparkMapReduceScoringHandler handler = buildCometScoringHandler(args[TANDEM_CONFIG_INDEX]);

        XTandemMain scoringApplication = handler.getApplication();


        setDebuggingCountMade(scoringApplication.getBooleanParameter(SparkUtilities.DO_DEBUGGING_CONFIG_PROPERTY, false));
        CometScoringAlgorithm comet = (CometScoringAlgorithm) scoringApplication.getAlgorithms()[0];

        Properties sparkProperties = SparkUtilities.getSparkProperties();
        String spectrumPath = scoringApplication.getSpectrumPath();
        JavaSparkContext currentContext = SparkUtilities.getCurrentContext();

        String spectra = SparkUtilities.buildPath(spectrumPath);
        int spectraToHandle = 1000;
        JavaPairRDD<String,String > subspectra = null; // FIX_THIS getPartitionSpectra(spectra, scoringApplication,spectraToHandle);

        Map<String, String> stringStringMap = subspectra.collectAsMap();
 
        new File("subspectra").mkdirs();
         for (String s : stringStringMap.keySet()) {
            File f = new File("subspectra/" + s + ".mzXML");
            FileUtilities.writeFile(f,stringStringMap.get(s));
         }


        //        long[] spectraCounts = new long[1];
//        spectraToScore = SparkUtilities.persistAndCount("Total Spectra",spectraToScore,spectraCounts);
//        long numberSpectra = spectraCounts[0];






        String fastaBase = scoringApplication.getDatabaseName();
        Path defaultPath = XTandemHadoopUtilities.getDefaultPath();
        String fasta = defaultPath.toString() + "/" + fastaBase + ".fasta";

         // key is uuid for file name
        // there are some duplicate proteins


        int proteinsToHandle = 1000;
        JavaPairRDD<String,String > subfastas = SparkSpectrumUtilities.partitionFastaFile(fasta,currentContext,proteinsToHandle);
    }

    /**
     * score with a join of a List of peptides
     *
     * @param args
     */
    public static void scoreWithFiles(String[] args) {




        long totalSpectra = 0;
        List<PairCounter> pairs = null;

        // Force PepXMLWriter to load
        PepXMLWriter foo = null;
        ElapsedTimer timer = new ElapsedTimer();
        ElapsedTimer totalTime = new ElapsedTimer();

        if (args.length < TANDEM_CONFIG_INDEX + 1) {
            System.out.println("usage sparkconfig configFile");
            return;
        }

        buildDesiredScoring(args);

        SparkUtilities.readSparkProperties(args[SPARK_CONFIG_INDEX]);

        SparkMapReduceScoringHandler handler = buildCometScoringHandler(args[TANDEM_CONFIG_INDEX]);

        XTandemMain scoringApplication = handler.getApplication();


        setDebuggingCountMade(scoringApplication.getBooleanParameter(SparkUtilities.DO_DEBUGGING_CONFIG_PROPERTY, false));
        CometScoringAlgorithm comet = (CometScoringAlgorithm) scoringApplication.getAlgorithms()[0];

        JavaSparkContext currentContext = SparkUtilities.getCurrentContext();
        
        Properties sparkProperties = SparkUtilities.getSparkProperties();
        String spectrumPath = scoringApplication.getSpectrumPath();

        String addedFiles = scoringApplication.getParameter("com.lordjoe.distributed.files", "");
        if(addedFiles.length() > 0)  {
            String[] split = addedFiles.split(";");
            for (int i = 0; i < split.length; i++) {
                String s = split[i];
                currentContext.addFile(s);
             }
        }

//        String spectra = SparkUtilities.buildPath(spectrumPath);
//        int spectraToHandle = 1000;
//        JavaPairRDD<String,String > subspectra = currentContext.wholeTextFiles("converted");
//        Map<String, String> stringStringMap = subspectra.collectAsMap();
//
//        new File("subspectra").mkdirs();
//        for (String s : stringStringMap.keySet()) {
//            File f = new File("subspectra/" + s + ".mzXML");
//            FileUtilities.writeFile(f,stringStringMap.get(s));
//        }
//

        JavaRDD<String> fastas = getFastaSplits(handler);

        JavaRDD<String> spectraData = getMZXMLSplits(handler);

        JavaRDD<Tuple2<String, String>> scoringPairs = buildScoringPairs(  fastas, spectraData) ;

        JavaRDD<String> pepXMLS = scoringPairs.map(new CometScoringFunction());

        List<String> scoringMap = pepXMLS.collect();

        int index = 1;
        new File("scores").mkdirs();
        for (String s : scoringMap ) {
            File f = new File("scores/" + "score" + index++ + ".pep.xml");
            FileUtilities.writeFile(f,s);
        }


        JavaPairRDD<String,IScoredScan> scores = pepXMLS.flatMapToPair(new PepxmlParsingFunction());



        //        long[] spectraCounts = new long[1];
//        spectraToScore = SparkUtilities.persistAndCount("Total Spectra",spectraToScore,spectraCounts);
//        long numberSpectra = spectraCounts[0];






        String fastaBase = scoringApplication.getDatabaseName();
        Path defaultPath = XTandemHadoopUtilities.getDefaultPath();
        String fasta = defaultPath.toString() + "/" + fastaBase + ".fasta";

        // key is uuid for file name
        // there are some duplicate proteins


        int proteinsToHandle = 1000;
        JavaPairRDD<String,String > subfastas = SparkSpectrumUtilities.partitionFastaFile(fasta,currentContext,proteinsToHandle);
    }

    public static JavaRDD<Tuple2<String, String>> buildScoringPairs( JavaRDD<String> fastas,JavaRDD<String> spectraData)
    {
        JavaPairRDD<String, String> cartesian = fastas.cartesian(spectraData);
       return  cartesian.map( IdentityFunction.INSTANCE);

    }


    public static JavaRDD<String>  getFastaSplits(SparkMapReduceScoringHandler app)   {
        JavaSparkContext currentContext = SparkUtilities.getCurrentContext();
        JavaPairRDD<String, String> testFiles1 = currentContext.wholeTextFiles("fastasSmall");
        return testFiles1.values();
    }

    public static JavaRDD<String>  getMZXMLSplits(SparkMapReduceScoringHandler app)   {
        JavaSparkContext currentContext = SparkUtilities.getCurrentContext();
        JavaPairRDD<String, String> testFiles1 = currentContext.wholeTextFiles("convertedSmall");
        return testFiles1.values();
    }

    /**
     * call with args like or20080320_s_silac-lh_1-1_11short.mzxml in Sample2
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        AccumulatorUtilities.setFunctionsLoggedByDefault(false);
        scoreWithFiles(args);

    }
}