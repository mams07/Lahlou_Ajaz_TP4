package com.opstty;

import com.opstty.job.DisCon;
import com.opstty.job.ListSpecies;
import com.opstty.job.NumTreeSpe;
import com.opstty.job.WordCount;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("Discon", DisCon.class,
                    "A map/reduce that make a list of distinct containing trees in this file.");
            programDriver.addClass("List species", ListSpecies.class,
                    "A map/reduce that make a list of species of trees in this file.");
            programDriver.addClass("Number of tree species", NumTreeSpe.class,
                    "A map/reduce that make a list of the number of tree per species in this file.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
