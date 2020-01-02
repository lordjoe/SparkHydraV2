package org.systemsbiology.xtandem.fdr;

import org.junit.Assert;
import org.junit.Test;
import org.systemsbiology.xml.XMLUtilities;

/**
 * org.systemsbiology.xtandem.fdr.ProteinPepxmlParserTests
 * User: Steve
 * Date: 12/14/2018
 */
public class ProteinPepxmlParserTests {
    public static final String TEST_XML = "  <search_hit hit_rank=\"2\" peptide=\"AGGEAER\" peptide_prev_aa=\"K\" peptide_next_aa=\"V\" protein=\"sp|Q9UXW5|PURQ_PYRAB\" num_tot_proteins=\"2\" num_matched_ions=\"4\" tot_num_ions=\"12\" calc_neutral_pep_mass=\"688.314017\" massdiff=\"1.033063\" num_tol_term=\"2\" num_missed_cleavages=\"0\" num_matched_peptides=\"3176\">\n"+
            "    <alternative_protein protein=\"sp|O59619|PURQ_PYRHO\"/>\n"+
            "    <search_score name=\"xcorr\" value=\"0.766\"/>\n"+
            "    <search_score name=\"deltacn\" value=\"0.074\"/>\n"+
            "    <search_score name=\"deltacnstar\" value=\"0.000\"/>\n"+
            "    <search_score name=\"spscore\" value=\"67.3\"/>\n"+
            "    <search_score name=\"sprank\" value=\"48\"/>\n"+
            "    <search_score name=\"expect\" value=\"1.08E+001\"/>\n"+
            "   </search_hit>";
    public static final String EXPEXTED1 = "  <search_hit hit_rank=\"13\" peptide=\"AGGEAER\" peptide_prev_aa=\"K\" peptide_next_aa=\"V\" protein=\"sp|Q9UXW5|PURQ_PYRAB\" num_tot_proteins=\"2\" num_matched_ions=\"4\" tot_num_ions=\"12\" calc_neutral_pep_mass=\"688.314017\" massdiff=\"1.033063\" num_tol_term=\"2\" num_missed_cleavages=\"0\" num_matched_peptides=\"3176\">\n"+
            "    <alternative_protein protein=\"sp|O59619|PURQ_PYRHO\"/>\n"+
            "    <search_score name=\"xcorr\" value=\"0.766\"/>\n"+
            "    <search_score name=\"deltacn\" value=\"0.074\"/>\n"+
            "    <search_score name=\"deltacnstar\" value=\"0.000\"/>\n"+
            "    <search_score name=\"spscore\" value=\"67.3\"/>\n"+
            "    <search_score name=\"sprank\" value=\"48\"/>\n"+
            "    <search_score name=\"expect\" value=\"1.08E+001\"/>\n"+
            "   </search_hit>";

    @Test
    public void testSubstitute() throws Exception {
        String result1 = XMLUtilities.substituteAttributeValue(TEST_XML,"hit_rank","13");
         Assert.assertEquals(result1,EXPEXTED1);
    }
}
