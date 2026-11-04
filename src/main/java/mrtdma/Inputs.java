package mrtdma;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import mrtdma.Records.*;

public final class Inputs {
    private Inputs() {}
    /** Exact Map count without rewriting immutable input coefficient records.
     * Existing one-file-per-Map behavior is retained when file counts match.
     * SequenceFileRecordReader synchronizes split boundaries to complete records.
     */
    public static final class WholeSequenceFiles<K,V> extends SequenceFileInputFormat<K,V> {
        protected boolean isSplitable(JobContext ctx,org.apache.hadoop.fs.Path path) { return false; }
        public List<InputSplit> getSplits(JobContext ctx) throws IOException {
            int expected=ctx.getConfiguration().getInt("tdma.maps",-1);
            List<FileStatus> files=listStatus(ctx);
            files.sort(Comparator.comparing(f->f.getPath().toString()));
            if(expected<files.size()||files.isEmpty())
                throw new IOException("Need at least one nonempty input split per file: "+files.size()+" files, "+expected+" Maps");
            long total=0;
            for(FileStatus f:files) { if(f.isDirectory()||f.getLen()==0)throw new IOException("Empty/directory input: "+f.getPath());total+=f.getLen(); }
            int[] counts=new int[files.size()];Arrays.fill(counts,1);
            int remaining=expected-files.size(),assigned=0;
            double[] fractions=new double[files.size()];
            for(int i=0;i<files.size();i++) {
                double share=(double)remaining*files.get(i).getLen()/total;
                int extra=(int)Math.floor(share);counts[i]+=extra;assigned+=extra;fractions[i]=share-extra;
            }
            for(int j=assigned;j<remaining;j++) {
                int best=0;for(int i=1;i<fractions.length;i++)if(fractions[i]>fractions[best])best=i;
                counts[best]++;fractions[best]=-1;
            }
            List<InputSplit> splits=new ArrayList<>();
            for(int i=0;i<files.size();i++) {
                FileStatus f=files.get(i);FileSystem fs=f.getPath().getFileSystem(ctx.getConfiguration());
                for(int k=0;k<counts[i];k++) {
                    long start=f.getLen()*k/counts[i],end=f.getLen()*(k+1)/counts[i];
                    if(end<=start)throw new IOException("Requested too many splits for "+f.getPath());
                    BlockLocation[] locations=fs.getFileBlockLocations(f,start,end-start);
                    String[] hosts=locations.length==0?new String[0]:locations[0].getHosts();
                    splits.add(new FileSplit(f.getPath(),start,end-start,hosts));
                }
            }
            if(splits.size()!=expected)throw new IOException("Exact input split count mismatch");
            return splits;
        }
    }
    public static final class TileSplit extends InputSplit implements Writable {
        long start,end;
        public TileSplit() {}
        TileSplit(long start,long end) { this.start=start; this.end=end; }
        public long getLength() { return end-start; }
        public String[] getLocations() { return new String[0]; }
        public void write(DataOutput out) throws IOException { out.writeLong(start); out.writeLong(end); }
        public void readFields(DataInput in) throws IOException { start=in.readLong(); end=in.readLong(); }
    }
    public static final class InitialTiles extends InputFormat<LongWritable,Tile> {
        public List<InputSplit> getSplits(JobContext ctx) {
            Configuration c=ctx.getConfiguration(); long total=new Driver.Geometry(c).tiles();
            int maps=(int)Math.min(total,c.getInt("tdma.maps",40));
            List<InputSplit> result=new ArrayList<>();
            for(int i=0;i<maps;i++) result.add(new TileSplit(total*i/maps,total*(i+1)/maps));
            return result;
        }
        public RecordReader<LongWritable,Tile> createRecordReader(InputSplit ignored,TaskAttemptContext ctx) {
            return new RecordReader<LongWritable,Tile>() {
                long position,start,end; final LongWritable key=new LongWritable(); final Tile value=new Tile();
                public void initialize(InputSplit split,TaskAttemptContext c) { TileSplit s=(TileSplit)split; start=position=s.start; end=s.end; }
                public boolean nextKeyValue() { if(position==end) return false; key.set(position++); return true; }
                public LongWritable getCurrentKey() { return key; }
                public Tile getCurrentValue() { return value; }
                public float getProgress() { return (float)(position-start)/Math.max(1,end-start); }
                public void close() {}
            };
        }
    }
    /** One map per sorted reducer file; complete line boundaries never cross a split. */
    public static final class BoundaryLines extends FileInputFormat<LongWritable,Bundle> {
        protected boolean isSplitable(JobContext c,org.apache.hadoop.fs.Path p) { return false; }
        public List<InputSplit> getSplits(JobContext c) throws IOException {
            List<InputSplit> splits=super.getSplits(c);
            splits.sort(Comparator.comparing(s->((FileSplit)s).getPath().toString()));
            return splits;
        }
        public RecordReader<LongWritable,Bundle> createRecordReader(InputSplit s,TaskAttemptContext c) {
            return new RecordReader<LongWritable,Bundle>() {
                SequenceFile.Reader reader; final PartitionKey pending=new PartitionKey();
                final Boundary boundary=new Boundary(); final LongWritable line=new LongWritable();
                final Bundle bundle=new Bundle(); boolean has; long bytes; int parts;
                public void initialize(InputSplit split,TaskAttemptContext ctx) throws IOException {
                    FileSplit f=(FileSplit)split;
                    reader=new SequenceFile.Reader(ctx.getConfiguration(),SequenceFile.Reader.file(f.getPath()));
                    bytes=f.getLength();
                    parts=new Driver.Geometry(ctx.getConfiguration()).parts; bundle.rows=new double[6*parts]; has=reader.next(pending,boundary);
                }
                public boolean nextKeyValue() throws IOException {
                    if(!has) return false;
                    line.set(pending.line); int count=0;
                    do {
                        if(pending.part!=count||count>=parts) throw new IOException("Missing/duplicate/unordered partition for line "+line);
                        System.arraycopy(boundary.v,0,bundle.rows,6*count,6); count++;
                        has=reader.next(pending,boundary);
                    } while(has&&pending.line==line.get());
                    if(count!=parts) throw new IOException("Incomplete reduced line "+line+": "+count+"/"+parts);
                    return true;
                }
                public LongWritable getCurrentKey() { return line; }
                public Bundle getCurrentValue() { return bundle; }
                public float getProgress() throws IOException { return Math.min(1f,(float)reader.getPosition()/Math.max(1,bytes)); }
                public void close() throws IOException { if(reader!=null) reader.close(); }
            };
        }
    }
}
