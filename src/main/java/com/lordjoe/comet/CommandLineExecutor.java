package com.lordjoe.comet;

import java.io.*;

/**
 * com.lordjoe.comet.CommandLineExecutor
 * User: Steve
 * Date: 11/10/2018
 */
public class CommandLineExecutor implements Serializable {


    public static boolean executeCommandLine( String... args ) throws IOException {
        ProcessBuilder p = new ProcessBuilder(  args);
        //  System.out.println("Started EXE");

        Process process = p.start();
        try {
            String line;
             BufferedReader bri = new BufferedReader
                    (new InputStreamReader(process.getInputStream()));
            BufferedReader bre = new BufferedReader
                    (new InputStreamReader(process.getErrorStream()));
      //      while ((line = bri.readLine()) != null) {
      //          System.out.println(line);
      //      }
            bri.close();
            while ((line = bre.readLine()) != null) {
//                System.out.println(line);
            }
            bre.close();
            int result = process.waitFor();
            int returnVal = process.exitValue();
            return returnVal == 0;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);

        }
    }


}
