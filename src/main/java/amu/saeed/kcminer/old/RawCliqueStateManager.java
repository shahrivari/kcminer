package amu.saeed.kcminer.old;

public class RawCliqueStateManager implements OldCliqueStateManager {
    public OldCliqueState makeNew(int v, int[] neighbors) {
        OldCliqueState state = new OldCliqueState();
        state.clique = new int[] {v};
        state.extension = new int[neighbors.length];
        for (int i = 0; i < neighbors.length; i++)
            if (neighbors[i] > v)
                state.extension[state.extSize++] = neighbors[i];
        return state;
    }

    public OldCliqueState expand(OldCliqueState state, int w, int[] w_neighbors) {
        OldCliqueState newState = new OldCliqueState();
        newState.clique = new int[state.clique.length + 1];
        System.arraycopy(state.clique, 0, newState.clique, 0, state.clique.length);
        newState.clique[state.clique.length] = w;
        newState.extension = new int[state.extSize];
        int i = 0, j = 0;
        while (i < state.extSize && j < w_neighbors.length) {
            if (state.extension[i] < w_neighbors[j])
                i++;
            else if (state.extension[i] > w_neighbors[j])
                j++;
            else {
                if (state.extension[i] > w)
                    newState.extension[newState.extSize++] = state.extension[i];
                i++;
                j++;
            }
        }
        return newState;
    }

}
