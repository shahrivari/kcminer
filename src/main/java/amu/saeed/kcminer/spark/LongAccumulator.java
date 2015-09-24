package amu.saeed.kcminer.spark;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;

/**
 * Created by saeed on 6/10/15.
 */
public class LongAccumulator implements AccumulatorParam<Long> {
    public static Accumulator<Long> create() {
        return new Accumulator<Long>(0L, new LongAccumulator());
    }

    @Override public Long addAccumulator(Long aLong, Long aLong2) {
        return aLong + aLong2;
    }

    @Override public Long addInPlace(Long aLong, Long r1) {
        return aLong + r1;
    }

    @Override public Long zero(Long aLong) {
        return 0L;
    }
}
