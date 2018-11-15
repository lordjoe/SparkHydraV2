package com.lordjoe.comet;

import com.lordjoe.distributed.SparkUtilities;
import com.lordjoe.distributed.spark.accumulators.AbstractLoggingFunction;
import com.lordjoe.utilities.FileUtilities;
import org.apache.spark.SparkFiles;
import scala.Tuple2;

import java.io.File;
import java.util.List;
import java.util.UUID;

/**
 * com.lordjoe.comet.CometScoringFunction
 * User: Steve
 * Date: 11/11/2018
 */
public class CometScoringFunction extends AbstractLoggingFunction<Tuple2<String, String>, String> {

    private transient String baseParamters;

    @Override
    public String doCall(Tuple2<String, String> v1) throws Exception {
        String fasta = v1._1;
        String mzXML = v1._2;

        File fastaFile = createLocalFile(fasta,"fasta");
        FileUtilities.writeFile(fastaFile,fasta);
        File xmlFile = createLocalFile(mzXML,"mzXML");
        FileUtilities.writeFile(xmlFile,mzXML);
        String params = getBaseParams().replace("XXXSET_FAST_NAME_HEREXXX", fastaFile.getAbsolutePath());
        File paramsFile = createLocalFile(params,"params");
        FileUtilities.writeFile(paramsFile,params);
        File outFile = new File(xmlFile.getAbsolutePath().replace(".mzXML", ".pep.xml"));
        boolean success = CommandLineExecutor.executeCommandLine("comet",
                "-P" + paramsFile.getName(),
                xmlFile.getAbsolutePath()
        );
        String ret = null;
        if (success) {
            ret = FileUtilities.readInFile(outFile);
        }
        fastaFile.delete();
        xmlFile.delete();
        paramsFile.delete();
        return ret;

    }

    private File createLocalFile(String fasta,String extension) {
           String  name = UUID.randomUUID().toString() + "." + extension;
           return new File(name);
    }

    private void buildParamsFile() {
        if (baseParamters == null)
            baseParamters = getBaseParams();

    }

    private static String getBaseParams() {
        String path = SparkFiles.get("comet.base.params");

        path = path.replace("\\", "/");
        List<String> collect = SparkUtilities.getCurrentContext().textFile(path)
                .collect();
        StringBuilder sb = new StringBuilder();
        for (String s : collect) {
            sb.append(s);
            sb.append("\n");
        }

        return sb.toString();

    }
}
