package mrtdma;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import mrtdma.Records.*;
import mrtdma.TaskTiming.TimedMapper;
import static mrtdma.Driver.*;

/** Step2 task implementations; original arithmetic and task ownership preserved. */
public final class Step2 {
    private Step2() {}
    public static final class ReducedSolver extends TimedMapper<LongWritable,Bundle,PartitionKey,Ends> {
        Geometry g; MultipleOutputs<PartitionKey,Ends> extra;
        boolean nonempty;
        protected void setup(Context ctx) throws IOException,InterruptedException {
            g=new Geometry(ctx.getConfiguration());
            int shard=((DistributedBoundaryInput.OwnerSplit)ctx.getInputSplit()).owner;
            extra=new MultipleOutputs<>(ctx);
            // JobSubmitter reorders input splits by size. Record the actual mapping
            // instead of assuming the map task number equals the reducer shard.
            String file=String.format(Locale.ROOT,"part-m-%05d",ctx.getTaskAttemptID().getTaskID().getId());
            extra.write("shard",new Text(Integer.toString(shard)),new Text(file),"shards/part");
        }
        protected void map(LongWritable line,Bundle value,Context ctx) throws IOException,InterruptedException {
            nonempty=true;
            long componentClock=TaskTiming.start();
            int parts=g.parts,n=2*parts;
            double[] a=new double[n],b=new double[n],c=new double[n],d=new double[n];
            for(int i=0;i<parts;i++) {
                a[2*i]=value.rows[6*i]; c[2*i]=value.rows[6*i+1]; d[2*i]=value.rows[6*i+2];
                a[2*i+1]=value.rows[6*i+3]; c[2*i+1]=value.rows[6*i+4]; d[2*i+1]=value.rows[6*i+5];
                b[2*i]=b[2*i+1]=1.0;
            }
            if(g.axis==1) Numerics.thomas(a,b,c,d); else Numerics.cyclic(a,b,c,d);
            TaskTiming.computed(componentClock);
            for(int i=0;i<parts;i++) {
                if(!Double.isFinite(d[2*i])||!Double.isFinite(d[2*i+1])) throw new IOException("Non-finite interface solution");
                ctx.write(new PartitionKey(line.get(),i),new Ends(d[2*i],d[2*i+1]));
            }
            ctx.getCounter(COUNT,"REDUCED_LINES_SOLVED").increment(1);
            PlacementTrace.write(extra,"Step2Map",ctx,line.get(),-1);
        }
        protected void cleanup(Context ctx) throws IOException,InterruptedException {
            if(nonempty) ctx.getCounter(COUNT,"NONEMPTY_MAPS").increment(1);
            extra.close();
        }
    }
}
