package com.lordjoe.distributed.hydra.pepxml;

import org.systemsbiology.xtandem.IEquivalent;
import org.systemsbiology.xtandem.peptide.IPolypeptide;

/**
 * com.lordjoe.distributed.hydra.pepxml.SpectrumHit
 *
 * @author Steve Lewis
 * @date 5/18/2015
 */
public class SpectrumHit implements IEquivalent<SpectrumHit> {
    public final String id;
    private String proteinId;
    public final int hit_Rank;
    public final double hypderscore;
    public final IPolypeptide peptide;

    public SpectrumHit(final String pId, final double pHypderscore, int pRank, final IPolypeptide pPeptide) {
        id = pId;
        hypderscore = pHypderscore;
        hit_Rank = pRank;
        peptide = pPeptide;
    }

    public String getProteinId() {
        return proteinId;
    }

    public void setProteinId(String proteinId) {
        this.proteinId = proteinId;
    }

    @Override
    public String toString() {
        return "SpectrumHit{" +
                "id='" + id + '\'' +
                ", peptide='" + peptide + '\'' +
                ", hypderscore=" + hypderscore +
                '}';
    }

    @Override
    public boolean equivalent(SpectrumHit o) {
        if (!peptide.equivalent(o.peptide))
            return peptide.equivalent(o.peptide);   // take a look on the way out
        if (hit_Rank != o.hit_Rank)
            return hit_Rank == o.hit_Rank;
        //noinspection RedundantIfStatement
        double del = hypderscore - o.hypderscore;
        if (Math.abs(del) > 0.01)
            return false;
        return true;
    }
}
