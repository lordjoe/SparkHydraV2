package com.lordjoe.comet;

import com.lordjoe.distributed.hydra.pepxml.PepxmlParser;
import com.lordjoe.distributed.hydra.pepxml.SpectrumQuery;
import com.lordjoe.distributed.spark.accumulators.AbstractLoggingPairFlatMapFunction;
import org.systemsbiology.xtandem.scoring.IScoredScan;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;

/**
 * com.lordjoe.comet.PepxmlParsingFunction
 * User: Steve
 * Date: 11/11/2018
 */
public class PepxmlParsingFunction extends AbstractLoggingPairFlatMapFunction<String, String, IScoredScan> {
    public static final PepxmlParsingFunction[] EMPTY_ARRAY = {};

    @Override
    public Iterator<Tuple2<String, IScoredScan>> doCall(String o) throws Exception {
        PepxmlParser pp = new PepxmlParser(o.split("\n"));
        Map<String, SpectrumQuery> qureies = pp.getQueries();
        if(true)
            throw new UnsupportedOperationException("Fix This"); // ToDo
        return null;
    }
}
