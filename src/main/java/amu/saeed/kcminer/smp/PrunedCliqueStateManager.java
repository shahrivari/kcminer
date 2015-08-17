package amu.saeed.kcminer.smp;

public class PrunedCliqueStateManager implements CliqueState.CliqueStateManager {
    final public CliqueState makeNew(int v, int[] neighbors) {
        CliqueState state = new CliqueState();
        state.subgraph = new int[]{v};
        state.extension = neighbors.clone();
        state.extSize = state.extension.length;
        return state;
    }

    final public CliqueState expand(CliqueState state, int w, int[] w_neighbors) {
        CliqueState newState = new CliqueState();
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
                newState.extension[newState.extSize++] = state.extension[i];
                i++;
                j++;
            }
        }
        return newState;
    }

}
