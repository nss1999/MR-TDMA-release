package mrtdma;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import mrtdma.Records.*;
import mrtdma.TaskTiming.TimedMapper;
import mrtdma.TaskTiming.TimedReducer;
import static mrtdma.Driver.*;

/** Step3 task implementations; original arithmetic and task ownership preserved. */
public final class Step3 {
    private Step3() {}
    /** Algorithm 5: attach boundary values; do not reconstruct interior values. */
    public static final class AttachBoundaries extends TimedMapper<PartitionKey,State,ReconstructionKey,Update> {
        Geometry g; MapFile.Reader boundaries;
        boolean nonempty;int owner=-1;MultipleOutputs<ReconstructionKey,Update> trace;
        final PartitionKey cursor=new PartitionKey(); final Ends ends=new Ends(); boolean have=false;
        protected void setup(Context ctx) throws IOException {
            g=new Geometry(ctx.getConfiguration());
            trace=new MultipleOutputs<>(ctx);
        }
        private void selectOwner(Context ctx,long line)throws IOException {
            int maps=ctx.getConfiguration().getInt("tdma.maps",0);
            int selected=DistributedBoundaryInput.owner(line,g.lines(),maps);
            if(selected==owner)return;
            if(boundaries!=null)boundaries.close();
            String file=ctx.getConfiguration().get("tdma.interface.file."+selected);
            if(file==null)throw new IOException("No interface file for owner "+selected);
            File local=new File("tdma-boundaries",file).getCanonicalFile();
            if(!local.isDirectory()||!new File(local,"data").isFile()||!new File(local,"index").isFile())throw new IOException("Missing localized boundary MapFile "+local);
            boundaries=new MapFile.Reader(new Path(local.toURI()),ctx.getConfiguration());
            if(owner<0)ctx.getCounter(COUNT,"CACHE_LOCAL_MAP_TASKS").increment(1);
            ctx.getCounter("MR_TDMA_DISTRIBUTED","BOUNDARY_OWNER_FILES_OPENED").increment(1);owner=selected;have=false;
        }

        protected void map(PartitionKey key,State state,Context ctx) throws IOException,InterruptedException {
            nonempty=true;selectOwner(ctx,key.line);
            if(!have) {
                PartitionKey found=(PartitionKey)boundaries.getClosest(key,ends);
                if(found==null) throw new IOException("Missing interface "+key);
                cursor.line=found.line; cursor.part=found.part; have=true;
            }
            while(cursor.compareTo(key)<0) if(!boundaries.next(cursor,ends)) throw new IOException("Interface EOF at "+key);
            if(!cursor.equals(key)) throw new IOException("Interface mismatch "+cursor+"/"+key);
            int[] point=g.point(key);
            ctx.write(new ReconstructionKey(g.tileId(point[0],point[1],point[2]),key),new Update(state,ends));
            ctx.getCounter(COUNT,"CACHE_PARTITIONS_ATTACHED").increment(1);
            PlacementTrace.write(trace,"Step3Map",ctx,key.line,key.part);
        }
        protected void cleanup(Context ctx) throws IOException,InterruptedException {
            if(nonempty) ctx.getCounter(COUNT,"NONEMPTY_MAPS").increment(1);
            try { if(boundaries!=null) boundaries.close(); } finally { trace.close(); }
        }
    }
    public static final class TilePartitioner extends Partitioner<ReconstructionKey,Update> {
        public int getPartition(ReconstructionKey key,Update value,int count) { return (Long.hashCode(key.tile)&Integer.MAX_VALUE)%count; }
    }
    /** Algorithm 6: one reduce group per subsystem; retain only one output tile. */
    public static final class ReconstructAndAssemble extends TimedReducer<ReconstructionKey,Update,LongWritable,Tile> {
        Geometry g,nextGeometry; Numerics.Physics physics;
        MultipleOutputs<LongWritable,Tile> extra;
        long currentTile=-1; double[] values; BitSet seen; int count;
        SpatialCoefficientOutputs spatialOutputs;
        protected void setup(Context ctx) throws IOException {
            g=new Geometry(ctx.getConfiguration());
            physics=new Numerics.Physics(g.grid.nx,g.grid.ny,g.grid.nz);
            extra=new MultipleOutputs<>(ctx);
            if(g.axis>0) {
                nextGeometry=new Geometry(g.grid,g.b,g.axis-1);
                if(ctx.getConfiguration().getBoolean("tdma.spatial.input",false))
                    spatialOutputs=new SpatialCoefficientOutputs(ctx,nextGeometry,
                        ctx.getConfiguration().getInt("tdma.slice.width",0),ctx.getConfiguration().getInt("tdma.slice.height",0));
            }
        }
        protected void reduce(ReconstructionKey key,Iterable<Update> updates,Context ctx) throws IOException,InterruptedException {
            if(currentTile!=key.tile) {
                flushTile(ctx); currentTile=key.tile;
                int[] t=g.tile(currentTile);
                values=new double[g.sx(t[0])*g.sy(t[1])*g.sz(t[2])]; seen=new BitSet(values.length); count=0;
            }
            Iterator<Update> it=updates.iterator();
            if(!it.hasNext()) throw new IOException("Empty subsystem "+key);
            Update update=it.next(); State state=update.state; Ends ends=update.ends;
            if(it.hasNext()) throw new IOException("Duplicate subsystem "+key);
            int[] point=g.point(new PartitionKey(key.line,key.part));
            if(g.tileId(point[0],point[1],point[2])!=currentTile) throw new IOException("Wrong tile "+key);
            int[] t=g.tile(currentTile); int ny=g.sy(t[1]),nz=g.sz(t[2]);
            int expected=Math.min(g.b,g.length-key.part*g.b);
            if(state.n!=expected) throw new IOException("Wrong subsystem length "+key);
            int offset=((point[0]%g.b)*ny+point[1]%g.b)*nz+point[2]%g.b;
            int stride=g.axis==2?1:g.axis==1?nz:ny*nz;
            long componentClock=TaskTiming.start();
            for(int i=0;i<state.n;i++) {
                // Boundary endpoints are already solved in Step 2; Eq. (5) restores interiors.
                double solution=i==0?ends.first:i==state.n-1?ends.last:
                    state.d[i]-state.a[i]*ends.first-state.c[i]*ends.last;
                int at=offset+stride*i;
                if(at<0||at>=values.length||seen.get(at)) throw new IOException("Duplicate/invalid tile cell "+key+":"+at);
                if(!Double.isFinite(solution)) throw new IOException("Non-finite reconstruction");
                seen.set(at); values[at]=solution; count++;
            }
            TaskTiming.computed(componentClock);
            ctx.getCounter(COUNT,"PARTITIONS_RECONSTRUCTED_REDUCE").increment(1);
            ctx.getCounter(COUNT,"POINTS_RECONSTRUCTED_REDUCE").increment(state.n);
            PlacementTrace.write(extra,"Step3Reduce",ctx,key.line,key.part);
        }
        private void flushTile(Context ctx) throws IOException,InterruptedException {
            if(currentTile<0) return;
            LongWritable key=new LongWritable(currentTile);
            int[] t=g.tile(currentTile); int nx=g.sx(t[0]),ny=g.sy(t[1]),nz=g.sz(t[2]);
            if(count!=values.length) throw new IOException("Incomplete tile "+key+" "+count+"/"+values.length);
            if(g.axis==0) {
                long componentClock=TaskTiming.start();
                for(int x=0;x<nx;x++) for(int y=0;y<ny;y++) for(int z=0;z<nz;z++)
                    values[(x*ny+y)*nz+z]+=physics.theta(t[0]*g.b+x,t[1]*g.b+y,t[2]*g.b+z);
                TaskTiming.computed(componentClock);
            }
            ctx.write(key,new Tile(values));
            if(nextGeometry!=null) prepareTile(nextGeometry,physics,currentTile,values,false,
                (part,coeff)-> {
                    if(spatialOutputs==null) extra.write("coefficients",part,coeff,"coefficients/part");
                    else spatialOutputs.write(part,coeff);
                });
            ctx.getCounter(COUNT,"POINTS_ASSEMBLED").increment(values.length);
        }
        protected void cleanup(Context ctx) throws IOException,InterruptedException {
            try { flushTile(ctx); }
            finally {
                try { if(spatialOutputs!=null) spatialOutputs.close(); }
                finally { extra.close(); }
            }
        }
    }

}
