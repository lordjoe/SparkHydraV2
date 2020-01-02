package com.lordjoe.comet;

import com.lordjoe.utilities.FileUtilities;
import org.junit.Assert;
import org.junit.Test;
import org.systemsbiology.xtandem.testing.MZXMLReader;

import java.io.*;

/**
 * com.lordjoe.comet.MZXMLTests
 * User: Steve
 * Date: 11/6/2018
 */
public class MZXMLTests {

    public static final String MZXML_FILE1 = "small.pwiz.1.1.mzXML";
    @Test
    public void testMD5() throws Exception {
        String[] strings = FileUtilities.readInResourceLines(MZXMLTests.class, MZXML_FILE1);

        LineNumberReader rdr = new LineNumberReader(new InputStreamReader(MZXMLTests.class.getResourceAsStream(MZXML_FILE1)));
         MZXMLFile file = MZXMLFileReader.readMZXMLFile(rdr);
         String indexed = file.makeIndexedString();
         String[] outStrings = indexed.split("\n");
       Assert.assertEquals(strings.length,outStrings.length) ;
        for (int i = 0; i < outStrings.length; i++) {
            String outString = outStrings[i].trim();
            String origginalString = strings[i].trim();
 //           System.out.println(outString + "\n" + origginalString);
            if(!outString.contains("<sha1>"))
                if(!equivalentString(outString ,origginalString )) {
                    System.out.println(outString + "\n" + origginalString);
                    Assert.assertTrue(equivalentString(outString, origginalString));

                }
           }
     }



    public static boolean equivalentString(String s1,String s2) {
        s1 = s1.replace(" ","");
        s2 = s2.replace(" ","");
        s1 = s1.replace("\t","");
        s2 = s2.replace("\t","");
        if(s1.equals(s2))
            return true;
        return false;
    }

    public static void main(String[] args) throws IOException {
        File tesfFile = new File(args[0]);
        File outFile = new File(args[1]);
        String[] strings = FileUtilities.readInLines(tesfFile);

        LineNumberReader rdr = new LineNumberReader(new FileReader(tesfFile));
        MZXMLFile file = MZXMLFileReader.readMZXMLFile(rdr);
        String data =  file.makeIndexedString( );
        FileUtilities.writeFile(outFile,data);
        String[] outStrings = data.split("\n");
        Assert.assertEquals(strings.length,outStrings.length) ;
        for (int i = 0; i < outStrings.length; i++) {
            String outString = outStrings[i].trim();
            String origginalString = strings[i].trim();
 //           System.out.println(outString + "\n" + origginalString);
            if(!outString.contains("<sha1>"))
                if(!equivalentString(outString ,origginalString )) {
                    System.out.println(outString + "\n" + origginalString);
                    Assert.assertTrue(equivalentString(outString, origginalString));
                    ;
                }
        }

    }
}
