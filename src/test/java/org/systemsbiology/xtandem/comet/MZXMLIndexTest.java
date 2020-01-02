package org.systemsbiology.xtandem.comet;

import com.lordjoe.comet.MZMLIndexGenerator;
import com.lordjoe.comet.MzXMLUtilities;
import com.lordjoe.utilities.FileUtilities;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.Map;

/**
 * org.systemsbiology.xtandem.comet.MZXMLIndexTest
 * User: Steve
 * Date: 12/19/2018
 */
public class MZXMLIndexTest {


    /**
     * mape sure masses of peptides agree with those Comet uses
     */
    @Test
    public void testPeptideMasses()
    {
        InputStream resourceAsStream = MZXMLIndexTest.class.getResourceAsStream("/test.mzXML");
        String data = FileUtilities.readInFile(resourceAsStream);

        resourceAsStream = MZXMLIndexTest.class.getResourceAsStream("/testIndexed.mzXML");
        String answerData = FileUtilities.readInFile(resourceAsStream);

        String indexed = MZMLIndexGenerator.generateIndex(data);
        String[] lines = indexed.split("\n");
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            
        }

    }

    /**
     * mape sure masses of peptides agree with those Comet uses
     */
    @Test
    public void testGetIndices()
    {
        InputStream resourceAsStream = MZXMLIndexTest.class.getResourceAsStream("/testIndexed.mzXML");
        String answerData = FileUtilities.readInFile(resourceAsStream);

        int indexOffset = MzXMLUtilities.getIndexOffset(answerData);
        Assert.assertEquals(indexOffset,200244);

        Map<Integer, Integer> scanIndices = MzXMLUtilities.getScanIndices(answerData);
        Assert.assertEquals(scanIndices.size(),28);
        Integer index24 = scanIndices.get(24);
        Assert.assertEquals(index24,(Integer)163563);

    }

    /**
     * mape sure masses of peptides agree with those Comet uses
     */
    @Test
    public void testGenerateIndices()
    {
        InputStream resourceAsStream = MZXMLIndexTest.class.getResourceAsStream("/testIndexed.mzXML");
        String answerData = FileUtilities.readInFile(resourceAsStream);
        answerData = answerData.replace("\r","");


        int indexOffset = MzXMLUtilities.getIndexOffset(answerData);

        Map<Integer, Integer> scanIndices = MzXMLUtilities.getScanIndices(answerData);


        String generated = MzXMLUtilities.generateIndexedData(answerData);

        FileUtilities.writeFile("generated.mzXML",generated) ;


        int generatedOffset = MzXMLUtilities.getIndexOffset(generated);
        Map<Integer, Integer> generatedIndices = MzXMLUtilities.getScanIndices(generated);

        Assert.assertEquals(generatedIndices.size(),scanIndices.size());

        for (Integer k : generatedIndices.keySet()) {
            int original = scanIndices.get(k);
            int regenerated = generatedIndices.get(k);
            Assert.assertEquals(original,regenerated);
        }

        Assert.assertEquals(indexOffset,generatedOffset);

    }



}
