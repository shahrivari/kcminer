package amu.saeed.kcminer.smp;

public class RawKlikStateMan implements KlikState.KlikStateMan {
    public KlikState makeNew(int v, int[] neighbors) {
        KlikState state = new KlikState();
        state.subgraph = new int[]{v};
        state.extension = new int[neighbors.length];
        for (int i = 0; i < neighbors.length; i++)
            if (neighbors[i] > v)
                state.extension[state.extSize++] = neighbors[i];
        return state;
    }

    public KlikState expand(KlikState state, int w, int[] w_neighbors) {
        KlikState newState = new KlikState();
        newState.subgraph = new int[state.subgraph.length + 1];
        System.arraycopy(state.subgraph, 0, newState.subgraph, 0, state.subgraph.length);
        newState.subgraph[state.subgraph.length] = w;
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
