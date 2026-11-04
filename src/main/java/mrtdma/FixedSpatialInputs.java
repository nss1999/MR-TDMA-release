package mrtdma;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import mrtdma.Records.Coefficients;
import mrtdma.Records.PartitionKey;

/** Fixed Map-count assignment of spatial input files; no coefficient arithmetic. */
public final class FixedSpatialInputs extends InputFormat<PartitionKey,Coefficients> {
    static final class Chunk {
        int region; Path path; long start,length;
        Chunk(int region,Path path,long start,long length) {
            this.region=region;this.path=path;this.start=start;this.length=length;
        }
    }
    public static final class FixedSplit extends InputSplit implements Writable {
        final List<Chunk> chunks=new ArrayList<>();
        long length; String[] hosts=new String[0];
        public FixedSplit() {}
        void add(Chunk chunk) {chunks.add(chunk);length+=chunk.length;}
        public long getLength(){return length;}
        public String[] getLocations(){return hosts;}
        public void write(DataOutput out)throws IOException {
            out.writeInt(chunks.size());
            for(Chunk c:chunks){out.writeInt(c.region);Text.writeString(out,c.path.toString());out.writeLong(c.start);out.writeLong(c.length);}
            out.writeInt(hosts.length);for(String h:hosts)Text.writeString(out,h);
        }
        public void readFields(DataInput in)throws IOException {
            chunks.clear();length=0;int count=in.readInt();
            if(count<1||count>1000000)throw new IOException("Invalid fixed spatial chunk count");
            for(int i=0;i<count;i++){
                int region=in.readInt();Path path=new Path(Text.readString(in));long start=in.readLong(),size=in.readLong();
                if(region<0||start<0||size<1)throw new IOException("Invalid fixed spatial chunk");
                add(new Chunk(region,path,start,size));
            }
            int n=in.readInt();if(n<0||n>1000)throw new IOException("Invalid fixed spatial locations");
            hosts=new String[n];for(int i=0;i<n;i++)hosts[i]=Text.readString(in);
        }
    }

    public List<InputSplit> getSplits(JobContext context)throws IOException {
        Configuration conf=context.getConfiguration();int maps=conf.getInt("tdma.maps",0);
        if(maps<1)throw new IOException("Fixed spatial Map count must be positive");
        // Validate spatial inventory before assigning files to a fixed Map count.
        List<SpatialInputs.RegionFiles> regions=SpatialInputs.inventory(context);
        List<Chunk> files=new ArrayList<>();Set<String> seen=new HashSet<>();long total=0;
        for(SpatialInputs.RegionFiles region:regions){
            for(int i=0;i<region.paths.length;i++){
                if(!seen.add(region.paths[i].toString()))throw new IOException("Duplicate spatial input file");
                files.add(new Chunk(region.region,region.paths[i],0,region.lengths[i]));total+=region.lengths[i];
            }
        }
        if(files.isEmpty())throw new IOException("No fixed spatial input files");
        List<Chunk> chunks=new ArrayList<>();
        if(files.size()<maps){
            // SequenceFileRecordReader synchronizes byte cuts to complete records.
            int remaining=maps-files.size(),assigned=0;int[] counts=new int[files.size()];Arrays.fill(counts,1);
            double[] fractions=new double[files.size()];
            for(int i=0;i<files.size();i++){
                double share=(double)remaining*files.get(i).length/total;
                int extra=(int)Math.floor(share);counts[i]+=extra;assigned+=extra;fractions[i]=share-extra;
            }
            for(int i=assigned;i<remaining;i++){
                int best=0;for(int k=1;k<files.size();k++)if(fractions[k]>fractions[best])best=k;
                counts[best]++;fractions[best]=-1;
            }
            for(int i=0;i<files.size();i++){
                Chunk f=files.get(i);
                for(int k=0;k<counts[i];k++){
                    long start=f.length*k/counts[i],end=f.length*(k+1)/counts[i];
                    if(end<=start)throw new IOException("Too many Maps for spatial file "+f.path);
                    chunks.add(new Chunk(f.region,f.path,start,end-start));
                }
            }
        }else chunks.addAll(files);
        // Deterministic largest-first byte balancing, with at least one chunk per Map.
        chunks.sort(Comparator.<Chunk>comparingLong(c->c.length).reversed()
            .thenComparing(c->c.path.toString()).thenComparingLong(c->c.start));
        List<FixedSplit> groups=new ArrayList<>();for(int i=0;i<maps;i++)groups.add(new FixedSplit());
        for(Chunk c:chunks){
            int best=0;for(int i=1;i<maps;i++)if(groups.get(i).length<groups.get(best).length)best=i;
            groups.get(best).add(c);
        }
        List<InputSplit> result=new ArrayList<>();
        for(FixedSplit split:groups){
            if(split.chunks.isEmpty())throw new IOException("Empty fixed spatial input assignment");
            Map<String,Long> weights=new HashMap<>();
            for(Chunk c:split.chunks){
                FileSystem fs=c.path.getFileSystem(conf);
                for(BlockLocation b:fs.getFileBlockLocations(c.path,c.start,c.length))
                    for(String h:b.getHosts())weights.merge(h,b.getLength(),Long::sum);
            }
            split.hosts=weights.entrySet().stream().sorted((a,b)->{
                int order=Long.compare(b.getValue(),a.getValue());return order==0?a.getKey().compareTo(b.getKey()):order;
            }).limit(3).map(Map.Entry::getKey).toArray(String[]::new);
            result.add(split);
        }
        return result;
    }

    public RecordReader<PartitionKey,Coefficients> createRecordReader(InputSplit input,TaskAttemptContext context){
        return new RecordReader<PartitionKey,Coefficients>(){
            FixedSplit split;TaskAttemptContext ctx;SequenceFileRecordReader<PartitionKey,Coefficients> reader;
            int index,width,height;long finished; Driver.Geometry geometry;
            public void initialize(InputSplit input,TaskAttemptContext context){
                split=(FixedSplit)input;ctx=context;Configuration c=ctx.getConfiguration();
                geometry=new Driver.Geometry(c);width=c.getInt("tdma.slice.width",0);height=c.getInt("tdma.slice.height",0);
            }
            public boolean nextKeyValue()throws IOException,InterruptedException{
                while(true){
                    if(reader==null){
                        if(index==split.chunks.size())return false;
                        Chunk c=split.chunks.get(index);reader=new SequenceFileRecordReader<>();
                        reader.initialize(new FileSplit(c.path,c.start,c.length,split.hosts),ctx);
                    }
                    if(reader.nextKeyValue()){
                        if(SpatialInputs.regionId(reader.getCurrentKey().line,geometry,width,height)!=split.chunks.get(index).region)
                            throw new IOException("Coefficient record outside its declared input region");
                        return true;
                    }
                    reader.close();reader=null;finished+=split.chunks.get(index++).length;
                }
            }
            public PartitionKey getCurrentKey()throws IOException,InterruptedException{return reader.getCurrentKey();}
            public Coefficients getCurrentValue()throws IOException,InterruptedException{return reader.getCurrentValue();}
            public float getProgress()throws IOException,InterruptedException{
                return Math.min(1f,(float)(finished+(reader==null?0:reader.getProgress()*split.chunks.get(index).length))/Math.max(1,split.length));
            }
            public void close()throws IOException{if(reader!=null)reader.close();}
        };
    }
}
