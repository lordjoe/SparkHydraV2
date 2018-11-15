package com.lordjoe.comet;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * com.lordjoe.comet.MSConvertRunner
 * User: Steve
 * Date: 11/10/2018
 */
public class MSConvertRunner implements Serializable {

    private static void runMSConvert(File file, File out)  throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("cmd msconvert.exe ");
        sb.append(file.getAbsolutePath());
        sb.append(" --mzXML -o ");
        sb.append(out.getAbsolutePath());
        sb.append(" --outfile  ");
        sb.append(file.getName());

        String command = sb.toString();
        CommandLineExecutor.executeCommandLine("msconvert",file.getAbsolutePath(),"--mzXML","-o",out.getAbsolutePath(),"--outfile",
                file.getName());
    }


    public static void main(String[] args)  throws  Exception {
        File dir = new File(args[0]);
        File out = new File(args[1]);
        out.mkdirs();
        File[] files = dir.listFiles();
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                File file = files[i];
                runMSConvert(file, out);
            }
        }
    }
}
