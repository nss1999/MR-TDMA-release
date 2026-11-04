package mrtdma;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import mrtdma.Records.Coefficients;
import mrtdma.Records.PartitionKey;

/** Bounded file handles for spatial routing only; no numerical operations. */
public final class SpatialCoefficientOutputs implements Closeable {
    public static final int MAX_OPEN_WRITERS=8;
    public static final String COUNTERS="SPATIAL_COEFFICIENT_IO";
    private final TaskInputOutputContext<?,?,?,?> context;
    private final Configuration conf;
    private final Path work;
    private final int width,height,reducer;
    private final Driver.Geometry geometry;
    private final CompressionCodec codec;
    private final LinkedHashMap<Integer,SequenceFile.Writer> open=new LinkedHashMap<>(16,0.75f,true);
    private long segment;
    private int peak;

    public SpatialCoefficientOutputs(TaskInputOutputContext<?,?,?,?> context,Driver.Geometry geometry,int width,int height) throws IOException {
        this.context=context;this.conf=context.getConfiguration();this.geometry=geometry;this.width=width;this.height=height;
        reducer=context.getTaskAttemptID().getTaskID().getId();
        try { work=FileOutputFormat.getWorkOutputPath(context); }
        catch(InterruptedException error) { Thread.currentThread().interrupt();throw new IOException("Interrupted resolving task output directory",error); }
        if(work==null) throw new IOException("No task-attempt output directory");
        codec=ReflectionUtils.newInstance(FileOutputFormat.getOutputCompressorClass(context,DefaultCodec.class),conf);
    }

    public void write(PartitionKey key,Coefficients value) throws IOException {
        int region=SpatialInputs.regionId(key.line,geometry,width,height);
        SequenceFile.Writer writer=open.get(region);
        if(writer==null) {
            if(open.size()==MAX_OPEN_WRITERS) {
                Iterator<Map.Entry<Integer,SequenceFile.Writer>> it=open.entrySet().iterator();
                SequenceFile.Writer oldest=it.next().getValue();it.remove();oldest.close();
                context.getCounter(COUNTERS,"WRITER_EVICTIONS").increment(1);
            }
            // A new segment follows an eviction. No append to a closed file,
            // no shared writer across Reducers, and no record is buffered here.
            String name=String.format(Locale.ROOT,"coefficients/region-%05d/part-r-%05d-seg-%07d",region,reducer,segment++);
            Path path=new Path(work,name);path.getFileSystem(conf).mkdirs(path.getParent());
            writer=SequenceFile.createWriter(conf,SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(PartitionKey.class),SequenceFile.Writer.valueClass(Coefficients.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK,codec),SequenceFile.Writer.progressable(context));
            open.put(region,writer);context.getCounter(COUNTERS,"OUTPUT_FILES_CREATED").increment(1);
            if(open.size()>MAX_OPEN_WRITERS) throw new IOException("Spatial writer limit exceeded");
            if(open.size()>peak) { context.getCounter(COUNTERS,"PER_TASK_PEAK_OPEN_WRITERS_SUM").increment(open.size()-peak);peak=open.size(); }
        }
        writer.append(key,value);
        context.getCounter(COUNTERS,"COEFFICIENT_RECORDS_WRITTEN").increment(1);
    }

    public void close() throws IOException {
        IOException failure=null;
        for(SequenceFile.Writer writer:open.values()) {
            try { writer.close(); }
            catch(IOException error) { if(failure==null) failure=error; else failure.addSuppressed(error); }
        }
        open.clear();
        if(failure!=null) throw failure;
    }
}
