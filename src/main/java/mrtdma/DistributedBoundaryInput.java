package mrtdma;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import mrtdma.Records.*;

/** Merge only boundary rows across reducer shards; no numerical transformation. */
public final class DistributedBoundaryInput extends InputFormat<LongWritable,Bundle> {
    public static int owner(long line,long total,int tasks) {
        if(line<0||line>=total||tasks<1||total<tasks)throw new IllegalArgumentException("Invalid line ownership");
        return (int)(((line+1)*tasks-1)/total);
    }
    public static final class OwnerSplit extends InputSplit implements Writable {
        public int owner;public long first,last;
        public OwnerSplit(){}
        OwnerSplit(int owner,long first,long last){this.owner=owner;this.first=first;this.last=last;}
        public long getLength(){return last-first;}
        public String[] getLocations(){return new String[0];}
        public void write(DataOutput out)throws IOException{out.writeInt(owner);out.writeLong(first);out.writeLong(last);}
        public void readFields(DataInput in)throws IOException{owner=in.readInt();first=in.readLong();last=in.readLong();}
    }
    public List<InputSplit> getSplits(JobContext context)throws IOException {
        int tasks=context.getConfiguration().getInt("tdma.maps",0);
        long total=new Driver.Geometry(context.getConfiguration()).lines();if(tasks<1||total<tasks)throw new IOException("Invalid owner split count");
        List<InputSplit> splits=new ArrayList<>();
        for(int i=0;i<tasks;i++)splits.add(new OwnerSplit(i,total*i/tasks,total*(i+1)/tasks));
        return splits;
    }
    static final class Cursor {
        final MapFile.Reader reader;final PartitionKey key=new PartitionKey();final Boundary value=new Boundary();
        Cursor(MapFile.Reader reader){this.reader=reader;}
    }
    public RecordReader<LongWritable,Bundle> createRecordReader(InputSplit split,TaskAttemptContext context) {
        return new RecordReader<LongWritable,Bundle>() {
            final List<Cursor> cursors=new ArrayList<>();
            final PriorityQueue<Cursor> queue=new PriorityQueue<>(Comparator.comparing(c->c.key));
            final LongWritable line=new LongWritable();final Bundle bundle=new Bundle();
            long next,last,first;int parts;TaskAttemptContext ctx;
            public void initialize(InputSplit input,TaskAttemptContext context)throws IOException {
                ctx=context;OwnerSplit s=(OwnerSplit)input;next=first=s.first;last=s.last;
                parts=new Driver.Geometry(ctx.getConfiguration()).parts;
                bundle.rows=new double[6*parts];
                Path root=new Path(ctx.getConfiguration().get("tdma.distributed.boundaries"));FileSystem fs=root.getFileSystem(ctx.getConfiguration());
                FileStatus[] dirs=fs.globStatus(new Path(root,"part-r-*"));
                int expected=ctx.getConfiguration().getInt("tdma.distributed.shards",0);
                if(dirs==null||dirs.length!=expected)throw new IOException("Incomplete boundary shard directory set");
                try {
                    for(FileStatus dir:dirs) {
                        Cursor c=new Cursor(new MapFile.Reader(dir.getPath(),ctx.getConfiguration()));cursors.add(c);
                        PartitionKey found=(PartitionKey)c.reader.getClosest(new PartitionKey(first,0),c.value);
                        if(found!=null&&found.line<last){c.key.line=found.line;c.key.part=found.part;queue.add(c);}
                        ctx.progress();
                    }
                } catch(IOException|RuntimeException ex){close();throw ex;}
            }
            public boolean nextKeyValue()throws IOException {
                if(next==last){if(!queue.isEmpty())throw new IOException("Unexpected surplus boundary rows");return false;}
                line.set(next);
                for(int q=0;q<parts;q++) {
                    Cursor c=queue.poll();
                    if(c==null||c.key.line!=next||c.key.part!=q)throw new IOException("Missing/duplicate boundary row for "+next+":"+q+"; got "+(c==null?"EOF":c.key));
                    System.arraycopy(c.value.v,0,bundle.rows,6*q,6);
                    if(c.reader.next(c.key,c.value)&&c.key.line<last)queue.add(c);
                    ctx.getCounter("MR_TDMA_DISTRIBUTED","BOUNDARY_PARTITIONS_GATHERED").increment(1);
                }
                next++;if((next&255)==0)ctx.progress();return true;
            }
            public LongWritable getCurrentKey(){return line;}
            public Bundle getCurrentValue(){return bundle;}
            public float getProgress(){return (float)(next-first)/Math.max(1,last-first);}
            public void close()throws IOException {
                IOException problem=null;for(Cursor c:cursors)try{c.reader.close();}catch(IOException ex){problem=ex;}
                cursors.clear();queue.clear();if(problem!=null)throw problem;
            }
        };
    }
}
