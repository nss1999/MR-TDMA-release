package mrtdma;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/** Optional clocks only. Values are cumulative task time, never wall-time shares. */
public final class TaskTiming {
    public static final String GROUP="MR_TDMA_COMPONENT_TIMING";
    private static boolean enabled,bound;
    private static long computation;
    private static final AtomicLong pendingIo=new AtomicLong(),pendingCalls=new AtomicLong(),pendingRead=new AtomicLong(),pendingWrite=new AtomicLong();
    private static volatile Counter io,calls,read,write;
    private TaskTiming() {}
    static void bind(TaskInputOutputContext<?,?,?,?> context) {
        enabled=context.getConfiguration().getBoolean("tdma.profile.components",false);
        if(!enabled)return;
        if(bound)throw new IllegalStateException("Component profiling requires one task per JVM");
        bound=true;
        io=context.getCounter(GROUP,"HDFS_API_NANOSECONDS");
        calls=context.getCounter(GROUP,"HDFS_API_CALLS");
        read=context.getCounter(GROUP,"HDFS_PROFILED_BYTES_READ");
        write=context.getCounter(GROUP,"HDFS_PROFILED_BYTES_WRITTEN");
        io.increment(pendingIo.getAndSet(0));calls.increment(pendingCalls.getAndSet(0));
        read.increment(pendingRead.getAndSet(0));write.increment(pendingWrite.getAndSet(0));
        context.getCounter(GROUP,"PROFILED_TASKS").increment(1);
    }
    public static long start() { return enabled?System.nanoTime():0; }
    public static void computed(long start) { if(start!=0)computation+=System.nanoTime()-start; }
    private static void add(Counter counter,AtomicLong pending,long value) { if(counter==null)pending.addAndGet(value);else counter.increment(value); }
    public static void io(long start,long bytesRead,long bytesWritten) {
        add(io,pendingIo,System.nanoTime()-start);add(calls,pendingCalls,1);
        if(bytesRead>0)add(read,pendingRead,bytesRead);
        if(bytesWritten>0)add(write,pendingWrite,bytesWritten);
    }
    private static void finish(TaskInputOutputContext<?,?,?,?> context) {
        if(enabled)context.getCounter(GROUP,"NUMERIC_COMPUTE_NANOSECONDS").increment(computation);
        // HDFS writes/close during the framework's output commit still update
        // the bound counters after Mapper/Reducer.run returns.
    }
    public static class TimedMapper<KI,VI,KO,VO> extends Mapper<KI,VI,KO,VO> {
        @Override public void run(Context context) throws IOException,InterruptedException {
            bind(context);try { super.run(context); } finally { finish(context); }
        }
    }
    public static class TimedReducer<KI,VI,KO,VO> extends Reducer<KI,VI,KO,VO> {
        @Override public void run(Context context) throws IOException,InterruptedException {
            bind(context);try { super.run(context); } finally { finish(context); }
        }
    }
}
