package mrtdma;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import mrtdma.Records.*;

/** Untimed, lossless coefficient routing into transverse rectangular regions. */
public final class SpatialRepack extends Configured implements Tool{
    static final String GROUP="SPATIAL_REPACK";
    public static final class Record implements Writable{
        final PartitionKey key=new PartitionKey();final Coefficients coefficients=new Coefficients();
        public void write(DataOutput out)throws IOException{key.write(out);coefficients.write(out);}
        public void readFields(DataInput in)throws IOException{key.readFields(in);coefficients.readFields(in);}
    }
    static long[] digest(Record record,MessageDigest hash,DataOutputBuffer buffer)throws IOException{
        buffer.reset();record.write(buffer);hash.reset();hash.update(buffer.getData(),0,buffer.getLength());byte[] d=hash.digest();ByteBuffer b=ByteBuffer.wrap(d);return new long[]{b.getLong(),b.getLong()};
    }
    public static final class Route extends Mapper<PartitionKey,Coefficients,LongWritable,Record>{
        Driver.Geometry g;int w,h;MessageDigest hash;final DataOutputBuffer buffer=new DataOutputBuffer();final Record record=new Record();
        protected void setup(Context c)throws IOException{
            Configuration conf=c.getConfiguration();g=new Driver.Geometry(conf);w=conf.getInt("tdma.slice.width",0);h=conf.getInt("tdma.slice.height",0);
            try{hash=MessageDigest.getInstance("SHA-256");}catch(Exception e){throw new IOException(e);}
        }
        protected void map(PartitionKey key,Coefficients value,Context c)throws IOException,InterruptedException{
            if(key.line<0||key.line>=g.lines()||key.part<0||key.part>=g.parts||value.n!=g.sz(key.part))throw new IOException("Invalid original record");
            record.key.line=key.line;record.key.part=key.part;record.coefficients.resize(value.n);
            System.arraycopy(value.a,0,record.coefficients.a,0,value.n);System.arraycopy(value.b,0,record.coefficients.b,0,value.n);
            System.arraycopy(value.c,0,record.coefficients.c,0,value.n);System.arraycopy(value.d,0,record.coefficients.d,0,value.n);
            long[] d=digest(record,hash,buffer);c.getCounter(GROUP,"INPUT_DIGEST_A").increment(d[0]);c.getCounter(GROUP,"INPUT_DIGEST_B").increment(d[1]);c.getCounter(GROUP,"INPUT_RECORDS").increment(1);
            c.write(new LongWritable(SpatialInputs.regionId(key.line,g,w,h)),record);
        }
    }
    public static final class WriteRegion extends Reducer<LongWritable,Record,PartitionKey,Coefficients>{
        MultipleOutputs<PartitionKey,Coefficients> output;Driver.Geometry g;int w,h;String[] prefixes;MessageDigest hash;final DataOutputBuffer buffer=new DataOutputBuffer();
        protected void setup(Context c)throws IOException{
            Configuration conf=c.getConfiguration();g=new Driver.Geometry(conf);w=conf.getInt("tdma.slice.width",0);h=conf.getInt("tdma.slice.height",0);prefixes=SpatialInputs.prefixes(g,w,h,"");output=new MultipleOutputs<>(c);
            try{hash=MessageDigest.getInstance("SHA-256");}catch(Exception e){throw new IOException(e);}
        }
        protected void reduce(LongWritable region,Iterable<Record> records,Context c)throws IOException,InterruptedException{
            int id=(int)region.get();if(id<0||id>=prefixes.length)throw new IOException("Invalid region");
            for(Record record:records){
                if(SpatialInputs.regionId(record.key.line,g,w,h)!=id)throw new IOException("Misrouted coefficient");
                long[] d=digest(record,hash,buffer);c.getCounter(GROUP,"OUTPUT_DIGEST_A").increment(d[0]);c.getCounter(GROUP,"OUTPUT_DIGEST_B").increment(d[1]);c.getCounter(GROUP,"OUTPUT_RECORDS").increment(1);
                output.write("region",record.key,record.coefficients,prefixes[id]);
            }
            c.getCounter(GROUP,"REGIONS_WRITTEN").increment(1);
        }
        protected void cleanup(Context c)throws IOException,InterruptedException{output.close();}
    }
    public int run(String[] args)throws Exception{
        Grid grid=new Grid(args,5,"N or Nx Ny Nz, then block width height original-input new-output");
        int offset=args.length-5;
        int block=Integer.parseInt(args[offset]),w=Integer.parseInt(args[offset+1]),h=Integer.parseInt(args[offset+2]);
        grid.validate(block);
        if(w<1||h<1||w>187||h>187)throw new IllegalArgumentException("Region dimensions must be in 1..187");
        Configuration c=new Configuration(getConf());
        grid.configure(c,block);c.setInt("tdma.axis",2);c.setInt("tdma.slice.width",w);c.setInt("tdma.slice.height",h);
        Driver.Geometry g=new Driver.Geometry(c);
        Path output=new Path(args[offset+4]);FileSystem fs=output.getFileSystem(c);if(fs.exists(output))throw new IOException("Output exists");
        Job j=Job.getInstance(c,"MR-TDMA lossless spatial preparation "+w+"x"+h);j.setJarByClass(SpatialRepack.class);j.setNumReduceTasks(SpatialInputs.regionCount(g,w,h));
        j.setMapperClass(Route.class);j.setReducerClass(WriteRegion.class);j.setMapOutputKeyClass(LongWritable.class);j.setMapOutputValueClass(Record.class);
        j.setInputFormatClass(SequenceFileInputFormat.class);FileInputFormat.addInputPath(j,new Path(args[offset+3],"part-*"));
        j.setOutputFormatClass(SequenceFileOutputFormat.class);j.setOutputKeyClass(PartitionKey.class);j.setOutputValueClass(Coefficients.class);FileOutputFormat.setOutputPath(j,output);
        FileOutputFormat.setCompressOutput(j,true);SequenceFileOutputFormat.setOutputCompressionType(j,SequenceFile.CompressionType.BLOCK);
        j.getConfiguration().setBoolean("mapreduce.map.speculative",false);j.getConfiguration().setBoolean("mapreduce.reduce.speculative",false);j.getConfiguration().setBoolean("mapreduce.map.output.compress",true);
        MultipleOutputs.addNamedOutput(j,"region",SequenceFileOutputFormat.class,PartitionKey.class,Coefficients.class);
        if(!j.waitForCompletion(true))throw new IOException("Spatial preparation failed");
        Counters counters=j.getCounters();long expected=g.partitions();
        if(counters.findCounter(GROUP,"INPUT_RECORDS").getValue()!=expected||counters.findCounter(GROUP,"OUTPUT_RECORDS").getValue()!=expected)throw new IOException("Coefficient count mismatch");
        for(String suffix:new String[]{"A","B"})if(counters.findCounter(GROUP,"INPUT_DIGEST_"+suffix).getValue()!=counters.findCounter(GROUP,"OUTPUT_DIGEST_"+suffix).getValue())throw new IOException("Serialized coefficients changed");
        if(counters.findCounter(GROUP,"REGIONS_WRITTEN").getValue()!=SpatialInputs.regionCount(g,w,h))throw new IOException("Missing regions");
        Map<String,Object> manifest=new LinkedHashMap<>();manifest.put("size",grid.cube()?(Object)grid.nx:grid.label());manifest.put("dimensions",grid.dimensions());manifest.put("block",block);manifest.put("width",w);manifest.put("height",h);manifest.put("regions",SpatialInputs.regionCount(g,w,h));manifest.put("records",expected);
        manifest.put("coefficientRecordsBitwiseUnchanged",true);manifest.put("verification","Record count and two64-bit modular sums of per-record SHA256 prefixes before/after routing; full-field numerical comparison follows");
        manifest.put("numericalComputationPerformed",false);manifest.put("jobId",j.getJobID().toString());
        try(OutputStream s=fs.create(new Path(output,"_spatial-manifest.json"),false)){new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(s,manifest);}
        return 0;
    }
    public static void main(String[] args)throws Exception{System.exit(ToolRunner.run(new SpatialRepack(),args));}
}
