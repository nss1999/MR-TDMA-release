package mrtdma;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import mrtdma.Records.*;
import mrtdma.TaskTiming.TimedMapper;
import mrtdma.TaskTiming.TimedReducer;
import static mrtdma.Driver.*;

/** Step1 task implementations; original arithmetic and task ownership preserved. */
public final class Step1 {
    private Step1() {}
    public static final class LinePartitioner extends Partitioner<PartitionKey,Coefficients> {
        public int getPartition(PartitionKey key,Coefficients value,int count) { return (int)Math.floorMod(31L*Long.hashCode(key.line)+key.part,count); }
    }
    /** Algorithm 3 Map: pass through the prepared coefficient records unchanged. */
    public static final class PartitionMapper extends TimedMapper<PartitionKey,Coefficients,PartitionKey,Coefficients> {
        boolean nonempty;
        protected void setup(Context ctx) {
            if(ctx.getConfiguration().getBoolean("tdma.spatial.input",false)&&
               !(ctx.getInputSplit() instanceof FixedSpatialInputs.FixedSplit))
                throw new IllegalArgumentException("Expected fixed-count spatial input assignment");
        }
        protected void map(PartitionKey key,Coefficients value,Context ctx) throws IOException,InterruptedException {
            nonempty=true;
            ctx.write(key,value);
            ctx.getCounter(COUNT,"PARTITIONS_MAPPED").increment(1);
            ctx.getCounter(COUNT,"COEFFICIENT_ROWS").increment(value.n);
        }
        protected void cleanup(Context ctx) { if(nonempty) ctx.getCounter(COUNT,"NONEMPTY_MAPS").increment(1); }
    }
    public static final class LocalReduction extends TimedReducer<PartitionKey,Coefficients,PartitionKey,Boundary> {
        MultipleOutputs<PartitionKey,Boundary> extra;
        protected void setup(Context ctx) { extra=new MultipleOutputs<>(ctx); }
        protected void reduce(PartitionKey key,Iterable<Coefficients> values,Context ctx) throws IOException,InterruptedException {
            Iterator<Coefficients> it=values.iterator();
            if(!it.hasNext()) throw new IOException("Empty partition "+key);
            Coefficients v=it.next(); if(it.hasNext()) throw new IOException("Duplicate partition "+key);
            long componentClock=TaskTiming.start();
            Numerics.reduce(v.a,v.b,v.c,v.d);
            TaskTiming.computed(componentClock);
            for(double x:v.d) if(!Double.isFinite(x)) throw new IOException("Non-finite local reduction");
            extra.write("state",key,new State(v),"state/part");
            ctx.write(key,new Boundary(v));
            ctx.getCounter(COUNT,"PARTITIONS_REDUCED").increment(1);
            PlacementTrace.write(extra,"Step1Reduce",ctx,key.line,key.part);
        }
        protected void cleanup(Context ctx) throws IOException,InterruptedException { extra.close(); }
    }
}
