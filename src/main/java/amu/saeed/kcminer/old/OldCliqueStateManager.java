package amu.saeed.kcminer.old;

/**
 * Created by Saeed on 8/18/2015.
 */
public interface OldCliqueStateManager {
    OldCliqueState makeNew(int v, int[] neighbors);

    OldCliqueState expand(OldCliqueState state, int w, int[] w_neighbors);
}
