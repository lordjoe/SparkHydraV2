package com.lordjoe.distributed.hydra.comet_spark;

import com.lordjoe.algorithms.Long_Formatter;
import com.lordjoe.algorithms.MapOfLists;
import com.lordjoe.distributed.PercentileFilter;
import com.lordjoe.distributed.SparkContextGetter;
import com.lordjoe.distributed.SparkUtilities;
import com.lordjoe.distributed.hydra.AddIndexToSpectrum;
import com.lordjoe.distributed.hydra.SparkScanScorer;
import com.lordjoe.distributed.hydra.SparkXTandemMain;
import com.lordjoe.distributed.hydra.comet.CometScoredScan;
import com.lordjoe.distributed.hydra.comet.CometScoringAlgorithm;
import com.lordjoe.distributed.hydra.comet.CometTheoreticalBinnedSet;
import com.lordjoe.distributed.hydra.fragment.BinChargeKey;
import com.lordjoe.distributed.hydra.scoring.PepXMLScoredScanWriter;
import com.lordjoe.distributed.hydra.scoring.SparkConsolidator;
import com.lordjoe.distributed.hydra.scoring.SparkMapReduceScoringHandler;
import com.lordjoe.distributed.hydra.test.TestUtilities;
import com.lordjoe.distributed.input.MultiMZXMLScanInputFormat;
import com.lordjoe.distributed.spark.accumulators.AbstractLoggingFunction;
import com.lordjoe.distributed.spark.accumulators.AccumulatorUtilities;
import com.lordjoe.distributed.spark.accumulators.BasicSparkAccumulators;
import com.lordjoe.distributed.spectrum.SparkSpectrumUtilities;
import com.lordjoe.distributed.tandem.LibraryBuilder;
import com.lordjoe.distributed.test.CometSpectraUse;
import com.lordjoe.utilities.ElapsedTimer;
import com.lordjoe.utilities.FileUtilities;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import java.io.PrintWriter;
import java.util.*;

/**
 * ccom.lordjoe.distributed.hydra.comet_spark.SparkCometCallingScorer
 * uses calls to comet directly
 * Date: 10/7/2014
 */
public class SparkCometCallingScorer {

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
        SparkCometCallingScorer.maxBinSpectra = maxBinSpectra;
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


    public static JavaRDD<IPolypeptide> readAllPeptides(Properties pSparkProperties, SparkMapReduceScoringHandler pHandler) {
        int max_proteins = 0;
        if (pSparkProperties.containsKey(MAX_PROTEINS_PROPERTY)) {
            max_proteins = Integer.parseInt(pSparkProperties.getProperty(MAX_PROTEINS_PROPERTY));
            System.err.println("Max Proteins " + max_proteins);
        }
        System.err.println("Max Proteins " + max_proteins);


        // handler.buildLibraryIfNeeded();
        // find all polypeptides and modified polypeptides
        JavaRDD<IPolypeptide> databasePeptides = pHandler.buildLibrary(max_proteins);


        // DEBUGGING why do we see more than one instance of interesting peptide
        //List<IPolypeptide> interesting1 = new ArrayList<IPolypeptide>();
        //databasePeptides = TestUtilities.findInterestingPeptides(databasePeptides, interesting1);

        if (isDebuggingCountMade())
            databasePeptides = SparkUtilities.persistAndCount("Database peptides", databasePeptides);

        // DEBUGGING why do we see more than one instance of interesting peptide
        //List<IPolypeptide> interesting2 = new ArrayList<IPolypeptide>();
        // databasePeptides = TestUtilities.findInterestingPeptides(databasePeptides, interesting2);

        return databasePeptides;
    }


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


    public static CometScoringHandler buildCometScoringHandler(String arg) {
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
        return new CometScoringHandler(configStr, false);
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

    private static void showAnalysisTotals(final long totalSpectra,
                                           final long peptidecounts,
                                           final long keyedSpectrumCounts,
                                           final long scoringCounts,
                                           final List<PairCounter> pPairs) {
        System.out.println("=========================================");
        System.out.println("========    Totals              =========");
        System.out.println("=========================================");
        System.out.println("Total Spectra " + totalSpectra);
        System.out.println("Keyed Spectra " + keyedSpectrumCounts);
        System.out.println("Total Peptides " + peptidecounts);
        long pairCount = 0;
        for (PairCounter pair : pPairs) {
            pairCount += pair.product;
        }
        System.out.println("Total Pairs " + SparkUtilities.formatLargeNumber(pairCount));
        System.out.println("Scored Pairs " + scoringCounts);
        System.out.println("Spectra times Peptides " + SparkUtilities.formatLargeNumber(totalSpectra * peptidecounts));

    }

    public static class MapToCometSpectrum extends AbstractLoggingFunction<IMeasuredSpectrum, CometScoredScan> {
        final CometScoringAlgorithm comet;

        public MapToCometSpectrum(final CometScoringAlgorithm pComet) {
            comet = pComet;
        }

        @Override
        public CometScoredScan doCall(final IMeasuredSpectrum pIMeasuredSpectrum) throws Exception {
            CometScoredScan ret = new CometScoredScan(pIMeasuredSpectrum, comet);
            return ret;
        }
    }


    /**
     * break one key into a split
     *
     * @param key     - original key
     * @param binsize how many in bin
     * @param maxSize max in bin
     * @return array of partitioned keys
     */
    protected static BinChargeKey[] splitKey(BinChargeKey key, long binsize, int maxSize) {
        int numberKeys = 1 + (int) (binsize / maxSize);
        BinChargeKey[] ret = new BinChargeKey[numberKeys];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new BinChargeKey(key.getCharge(), key.getMz(), i + 1);

        }
        return ret;
    }
//
//    protected static Set<Integer> temporaryExpedientToExtractIntegers(Map<BinChargeKey, Long> usedBinsMap) {
//        // temporary code for compatability
//        Set<Integer> usedBins = new HashSet<Integer>();
//        List<Long> binSizes = new ArrayList<Long>();
//
//        for (BinChargeKey key : usedBinsMap.keySet()) {
//            Long aLong1 = usedBinsMap.get(key);
//            binSizes.add(aLong1);
//
//            //   usedBins.add(v);
//            usedBins.add(key.getMzInt());
//        }
//
//        Collections.sort(binSizes);
//        Collections.reverse(binSizes); // biggest first
//        int index = 0;
//        System.out.println("Sizes of " + binSizes.size() + " bins");
//        for (Long binSize : binSizes) {
//            System.out.println("binsize = " + binSize);
//            if (index++ > 30)
//                break;
//        }
//        return usedBins;
//    }

    public static final int proteinsPerGroup = 1000;

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

        CometScoringHandler handler = buildCometScoringHandler(args[TANDEM_CONFIG_INDEX]);

        XTandemMain scoringApplication = handler.getApplication();


        setDebuggingCountMade(scoringApplication.getBooleanParameter(SparkUtilities.DO_DEBUGGING_CONFIG_PROPERTY, false));
        CometScoringAlgorithm comet = (CometScoringAlgorithm) scoringApplication.getAlgorithms()[0];

        Properties sparkProperties = SparkUtilities.getSparkProperties();
        String spectrumPath = scoringApplication.getSpectrumPath();
        JavaSparkContext currentContext = SparkUtilities.getCurrentContext();

        String spectra = SparkUtilities.buildPath(spectrumPath);
        int spectraToHandle = 1000;
        JavaPairRDD<String,String > subspectra = getPartitionSpectra(spectra,  spectraToHandle);

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
//        Map<String, String> stringStringMap = subfastas.collectAsMap();
//
//        new File("subfastas").mkdirs();
//        for (String s : stringStringMap.keySet()) {
//            File f = new File("subfastas/" + s + ".fasta");
//            FileUtilities.writeFile(f,stringStringMap.get(s));
//        }
//        //MZPartitioner partitioner = new MZPartitioner();
//        JavaRDD<IMeasuredSpectrum> spectraToScore = SparkScanScorer.getMeasuredSpectra(timer, sparkProperties, spectra, scoringApplication);
//
//        long[] spectrumCounts = new long[1];
//        spectraToScore = SparkUtilities.persistAndCount("Total Spectra", spectraToScore, spectrumCounts);
//        totalSpectra = spectrumCounts[0];
//
//        boolean countSpectraOnly = scoringApplication.getBooleanParameter(SparkXTandemMain.COUNT_SPECTRA_ONLY, false);
//
//        if (countSpectraOnly)   // eupa problem is struggling to do this
//        {
//            long count = spectraToScore.count();
//            System.out.println("Total Spectra " + count);
//            BasicSparkAccumulators.showAccumulators(totalTime);
//            return;
//        }
//
//        SparkContextGetter.reguireDefaultNumberPartitions((int) (totalSpectra / 10000));
//
//
//
//        long[] peptideCounts = new long[1];
    }




    public static JavaPairRDD<String,String> getPartitionSpectra( final String pSpectra, int spectraToHandle) {
           JavaSparkContext currentContext = SparkUtilities.getCurrentContext();
        String fileHeader = MultiMZXMLScanInputFormat.readMZXMLHeader(new Path(pSpectra));

        // read spectra
        JavaPairRDD<String, String> scans = SparkSpectrumUtilities.partitionAsMZXML(pSpectra,currentContext,spectraToHandle,fileHeader);
        // next line is for debugging
        // spectraToScore = SparkUtilities.realizeAndReturn(spectraToScore);
         return scans;
    }



    /**
     * Map a Spectrum to one of multiple bins
     *
     * @param keyedSpectra
     * @param usedBinsMap
     * @return
     */
    private static JavaPairRDD<BinChargeKey, CometScoredScan> remapSpectra(JavaPairRDD<BinChargeKey, CometScoredScan> keyedSpectra,
                                                                           final MapOfLists<Integer, BinChargeKey> splitKeys) {
        if (!splitKeys.containsEntryGreaterThanOne())
            return keyedSpectra; // no split needed
        return keyedSpectra.mapToPair(new PairFunction<Tuple2<BinChargeKey, CometScoredScan>, BinChargeKey, CometScoredScan>() {
            transient int index = 0;

            @Override
            public Tuple2<BinChargeKey, CometScoredScan> call(Tuple2<BinChargeKey, CometScoredScan> v) throws Exception {
                Integer mzI = v._1().getMzInt();
                CometScoredScan spectrum = v._2();
                List<BinChargeKey> binChargeKeys = splitKeys.get(mzI);
                int keyIndex = Math.abs(index++) % binChargeKeys.size();
                BinChargeKey newKey = binChargeKeys.get(keyIndex);
                return new Tuple2<BinChargeKey, CometScoredScan>(newKey, spectrum);
            }
        });
    }

//    /**
//     * get all keys we use for scoring
//     *
//     * @param keyedSpectra
//     * @return
//     */
//    private static Set<Integer> getUsedBins(JavaPairRDD<BinChargeKey, CometScoredScan> keyedSpectra) {
//        final Set<Integer> ret = new HashSet<Integer>();
//        JavaRDD<BinChargeKey> keys = keyedSpectra.keys();
//        List<BinChargeKey> collect = keys.collect();
//        for (BinChargeKey binChargeKey : collect) {
//            ret.add(binChargeKey.getMzInt());
//        }
//
//        return ret;
//    }


    /**
     * get all keys we use for scoring
     *
     * @param keyedSpectra
     * @return
     */
    private static Map<BinChargeKey, Long> getUsedBins(JavaPairRDD<BinChargeKey, CometScoredScan> keyedSpectra) {
        Map<BinChargeKey, Long> intermediate = keyedSpectra.countByKey();
        Map<BinChargeKey, Long> ret = new HashMap<BinChargeKey, Long>();
        for (BinChargeKey key : intermediate.keySet()) {
            Long item = (Long) intermediate.get(key);
            ret.put(key, item);
        }
        return ret;
    }


//    /**
//     * score with a join of a List of peptides
//     *
//     * @param args
//     */
//    public static void scoringUsingTheoreticalLists(String[] args) {
//        long totalSpectra = 0;
//        List<PairCounter> pairs = null;
//
//        // Force PepXMLWriter to load
//        PepXMLWriter foo = null;
//        ElapsedTimer timer = new ElapsedTimer();
//        ElapsedTimer totalTime = new ElapsedTimer();
//
//        if (args.length < TANDEM_CONFIG_INDEX + 1) {
//            System.out.println("usage sparkconfig configFile");
//            return;
//        }
//
//        buildDesiredScoring(args);
//
//        SparkUtilities.readSparkProperties(args[SPARK_CONFIG_INDEX]);
//
//        CometScoringHandler handler = buildCometScoringHandler(args[TANDEM_CONFIG_INDEX]);
//
//        XTandemMain scoringApplication = handler.getApplication();
//        setDebuggingCountMade(scoringApplication.getBooleanParameter(SparkUtilities.DO_DEBUGGING_CONFIG_PROPERTY, false));
//        CometScoringAlgorithm comet = (CometScoringAlgorithm) scoringApplication.getAlgorithms()[0];
//
//
//        Properties sparkProperties = SparkUtilities.getSparkProperties();
//        String spectrumPath = scoringApplication.getSpectrumPath();
//        String spectra = SparkUtilities.buildPath(spectrumPath);
//        JavaRDD<IMeasuredSpectrum> spectraToScore = SparkScanScorer.getMeasuredSpectra(timer, sparkProperties, spectra, scoringApplication);
//
//
//        JavaRDD<CometScoredScan> cometSpectraToScore = spectraToScore.map(new MapToCometSpectrum(comet));
//
//
//        cometSpectraToScore = countAndLimitSpectra(cometSpectraToScore);
//
//
//        JavaPairRDD<BinChargeKey, ITheoreticalSpectrumSet> binChargePeptidesX = getBinChargePeptides(sparkProperties, handler);
//
//        // this really just does a cast
//        JavaPairRDD<BinChargeKey, CometTheoreticalBinnedSet> binChargePeptides = SparkUtilities.castRDD(binChargePeptidesX, CometTheoreticalBinnedSet.class);
//
//        JavaPairRDD<BinChargeKey, ArrayList<CometTheoreticalBinnedSet>> keyedPeptides = SparkUtilities.mapToKeyedList(binChargePeptides);
//
//        timer.showElapsed("Mapped Peptides", System.err);
//
//        long[] counts = new long[1];
//        if (isDebuggingCountMade()) {
//            keyedPeptides = SparkUtilities.persistAndCountPair("Peptides as Theoretical Spectra", keyedPeptides, counts);
//        }
//
//        if (isDebuggingCountMade()) {
//            keyedPeptides = SparkUtilities.persistAndCountPair("Mapped Peptides", keyedPeptides, counts);
//        }
//        long peptidecounts = counts[0];
//
//        // these are spectra
//        JavaPairRDD<BinChargeKey, CometScoredScan> keyedSpectra = handler.mapMeasuredSpectrumToKeys(cometSpectraToScore);
//
//
//        if (isDebuggingCountMade()) {
//            keyedSpectra = SparkUtilities.persistAndCountPair("Mapped Spectra", keyedSpectra, counts);
//        }
//        long keyedSpectrumCounts = counts[0];
//
//
//        //        if (isDebuggingCountMade()) {
//        //            pairs = showBinPairSizes(keyedPeptides, keyedSpectra);
//        //        }
//
//        // find spectra-peptide pairs to score
//        JavaPairRDD<BinChargeKey, Tuple2<CometScoredScan, ArrayList<CometTheoreticalBinnedSet>>> binPairs = keyedSpectra.join(keyedPeptides);
//
//        if (isDebuggingCountMade())
//            binPairs = SparkUtilities.persistAndCountPair("Binned Pairs", binPairs, counts);
//
//        binPairs = binPairs.persist(StorageLevel.MEMORY_AND_DISK_SER());   // force comuptation before score
//        binPairs.count(); // force action to happen now
//
//        // now produce all peptide spectrum scores where spectrum and peptide are in the same bin
//        JavaRDD<? extends IScoredScan> bestScores = handler.scoreCometBinTheoreticalPairList(binPairs);  //  todo fix and restore
//
//        // combine scores from same scan
//        JavaRDD<? extends IScoredScan> cometBestScores = handler.combineScanScores(bestScores);
//
//        cometBestScores = cometBestScores.persist(StorageLevel.MEMORY_AND_DISK_SER());   // force comuptation after score
//        cometBestScores.count(); // force action to happen now
//
//
//        // todo combine score results from different bins
//
//        if (isDebuggingCountMade())
//            bestScores = SparkUtilities.persistAndCount("Best Scores", bestScores);
//
//        timer.showElapsed("built best scores", System.err);
//        //bestScores =  bestScores.persist(StorageLevel.MEMORY_AND_DISK());
//        // System.out.println("Total Scores " + bestScores.count() + " Scores");
//
//        XTandemMain application = scoringApplication;
//
//        // code using PepXMLWriter new uses tandem writer
//        PepXMLWriter pwrtr = new PepXMLWriter(application);
//        PepXMLScoredScanWriter pWrapper = new PepXMLScoredScanWriter(pwrtr);
//        SparkConsolidator consolidator = new SparkConsolidator(pWrapper, application);
//
//        //      BiomlReporter writer = new BiomlReporter(application);
//        //   SparkConsolidator consolidator = new SparkConsolidator(writer, application);
//
//
//        int numberScores = consolidator.writeScores(cometBestScores);
//        System.out.println("Total Scans Scored " + numberScores);
//
//        SparkAccumulators.showAccumulators(totalTime);
//
//        totalTime.showElapsed("Finished Scoring");
//
//        TestUtilities.closeCaseLoggers();
//        // purely debugging  code to see whether interesting peptides scored with interesting spectra
//        //TestUtilities.writeSavedKeysAndSpectra();
//    }


    public static JavaRDD<CometScoredScan> countAndLimitSpectra(JavaRDD<CometScoredScan> spectraToScore) {
        if (isDebuggingCountMade()) {
            long[] spectraCounts = new long[1];
            SparkUtilities.persistAndCount("Read Spectra", spectraToScore, spectraCounts);
            long spectraCount = spectraCounts[0];
            if (spectraCount > MAX_SPECTRA_TO_SCORE_IN_ONE_PASS) {
                int percentileKept = (int) ((100L * MAX_SPECTRA_TO_SCORE_IN_ONE_PASS) / spectraCount);
                System.err.println("Keeping " + percentileKept + "% spectra");
                spectraToScore = spectraToScore.filter(new PercentileFilter(percentileKept));
            }
        }
        return spectraToScore;
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
        scoringUsingCogroup(args);

    }
}
