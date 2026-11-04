package mrtdma;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/** Diagnostic output only; disabled during performance observations. */
public final class PlacementTrace {
    static final String HOST=hostname();
    static String hostname(){try{return InetAddress.getLocalHost().getHostName();}catch(Exception ex){return "unknown";}}
    @SuppressWarnings({"rawtypes","unchecked"})
    public static void write(MultipleOutputs extra,String stage,TaskInputOutputContext ctx,long line,int part)throws IOException,InterruptedException {
        if(!ctx.getConfiguration().getBoolean("tdma.placement.trace",false))return;
        String json="{\"stage\":\""+stage+"\",\"axis\":"+ctx.getConfiguration().getInt("tdma.axis",-1)+",\"line\":"+line+",\"part\":"+part+
            ",\"taskId\":\""+ctx.getTaskAttemptID().getTaskID()+"\",\"attemptId\":\""+ctx.getTaskAttemptID()+"\",\"host\":\""+HOST+"\",\"epochMillis\":"+System.currentTimeMillis()+"}";
        extra.write("placement",new Text(stage),new Text(json),"placement/part");
        ctx.getCounter("MR_TDMA_DISTRIBUTED","TRACE_"+stage).increment(1);
    }
}
