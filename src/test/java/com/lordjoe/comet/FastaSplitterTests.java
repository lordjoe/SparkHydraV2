package com.lordjoe.comet;

import org.junit.Assert;

import java.awt.image.ImagingOpException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashSet;
import java.util.Set;

/**
 * com.lordjoe.comet.FastaSplitterTests
 * User: Steve
 * Date: 11/8/2018
 */
public class FastaSplitterTests {
    public static Set<String> countFastaEntries(File fasta) throws IOException
    {
        Set<String> ret = new HashSet<>();
        LineNumberReader rdr = new LineNumberReader((new FileReader(fasta)));
        String line = rdr.readLine();
        while(line != null) {
            if(line.startsWith(">")) {
                ret.add(line);
            }
            line = rdr.readLine();
        }
        rdr.close();
        return ret;
    }

    public static Set<String> countPastaDirectoryEntries(File dir )  throws IOException
    {
        Set<String> ret = new HashSet<>();
        File[] files = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            if(file.getName().toLowerCase().endsWith(".fasta"))   {
                Set<String> fret = countFastaEntries(file);
                for (String s : fret) {
                    if(!ret.contains(s))
                        ret.add(s);
                    else
                        System.out.println("Duplicate entry " + s);
                }
            }
                
        }
        return ret;
    }


    public static void main(String[] args) throws IOException {
        File mainFasta = new File(args[0]) ;
        File splitDir = new File(args[1]) ;
        Set<String> nEntried = countFastaEntries(mainFasta) ;
        Set<String> nReadEntries = countPastaDirectoryEntries(splitDir) ;
        Assert.assertEquals(nEntried.size(),nReadEntries.size());

    }
}
