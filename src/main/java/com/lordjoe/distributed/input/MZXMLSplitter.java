package com.lordjoe.distributed.input;

import com.lordjoe.comet.MZXMLFile;
import com.lordjoe.distributed.SparkUtilities;
import com.lordjoe.utilities.FileUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.systemsbiology.hadoop.HadoopUtilities;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * com.lordjoe.distributed.input.MZXMLSplitter
 * User: Steve
 * Date: 9/24/2014
 */
public class MZXMLSplitter implements Serializable {
    public static final String MZMLFOOTER =
            "   </msRun>\n" +
                    "    <indexOffset>0</indexOffset>\n" +
                    "  <sha1>0</sha1>\n" +
                    "</mzXML>\n";

    public static final String END_SCANS = "</msRun>";
    public static final String SCAN_START = "<scan";
    public static final String SCAN_END = "</scan>";
    public static final String EXTENSION = ".mzXML";

    public static final int DEFAULT_SPECTRA_SPLIT = 1000;

    private static File  buildFile(File directory)
    {
        return new File(directory,UUID.randomUUID().toString() + EXTENSION);
    }


    /**
     * read header of mzXML file until first scan
     *
     * @param p path of the file
     * @return non-null header
     */
    public static String readMZXMLHeader(LineNumberReader rdr, String[] lineHolder) {
        try {
            StringBuilder sb = new StringBuilder();
            String line = rdr.readLine();
            sb.append(line);
            sb.append("\n");
            line = rdr.readLine();
            sb.append(line);
            sb.append("\n");
            if (!sb.toString().contains("<mzXML"))
                throw new IllegalArgumentException("path is not an mzXML file ");

            line = rdr.readLine();
            while (line != null) {
                sb.append(line);
                sb.append("\n");
                line = rdr.readLine();
                if (line.contains("<scan"))
                    break;
            }
            lineHolder[0] = line;
            return sb.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }


    public MZXMLSplitter() {
    }


    public static void splitMZXML(File input, File outDir, int maxScans) throws IOException {
         FileUtilities.expungeDirectory(outDir);
        boolean ok = outDir.mkdirs();
        String[] lineHolder = new String[1];
        LineNumberReader rdr = new LineNumberReader(new FileReader(input));
        String header = readMZXMLHeader(rdr, lineHolder);
        while (readAndSaveScans(outDir, rdr, header, lineHolder, maxScans)) ;
    }

    public static boolean readAndSaveScans(File outDir, LineNumberReader rdr, String header, String[] lineHolder, int maxScans) throws IOException {
        String line = lineHolder[0];
        if (line == null)
            return false;
        if (!line.contains(SCAN_START))
            return false;
        int numberScans = 0;
        StringBuilder sb = new StringBuilder();


        while (numberScans < maxScans) {
            MZXMLFile file = new MZXMLFile(header);
            int newScans = readNextScan(rdr, lineHolder, file, maxScans);
            if (newScans > 0)
                saveMZXML(outDir, file);
            if(newScans == 0)  {
                return false; // done
            }

            sb.setLength(0);
        }
        return true;
    }

    public static int readNextScan(LineNumberReader rdr, String[] lineHolder, MZXMLFile file, int maxScans) throws IOException {
        int numberScans = 0;
        int scanLevel = 0;
        String line = lineHolder[0];
        if (line == null)
            return 0;
        if (!line.contains(SCAN_START))
            return 0;
        numberScans++;
        scanLevel++;
        StringBuilder sb = new StringBuilder();
        while (line != null) {
            sb.append(line);
            sb.append("\n");
            line = rdr.readLine();
            if (line == null)
                break;
            if (line.contains(END_SCANS))
                break;
            if (line.contains(SCAN_END)) {
                scanLevel--;
            }
            if (line.contains(SCAN_START)) {
                numberScans++;
                if (scanLevel == 0) {
                    file.addScan(sb.toString());
                    sb.setLength(0);
                    if (numberScans >= maxScans)
                        break;
                    scanLevel++;
                }
            }
        }

        lineHolder[0] = line;
        return numberScans;
    }

    public static void saveMZXML(File outDir, MZXMLFile file)  throws IOException {
        File  out = buildFile(outDir);
        System.out.println(out.getName());
        StringBuilder sb = new StringBuilder();
        file.makeIndexedFile(sb);
        FileUtilities.writeFile(out,sb.toString());
    }

    public static void main(String[] args) throws IOException {
        File input = new File(args[0]);
        File outDir = new File(args[1]);
        splitMZXML(input, outDir, DEFAULT_SPECTRA_SPLIT);


    }
}
