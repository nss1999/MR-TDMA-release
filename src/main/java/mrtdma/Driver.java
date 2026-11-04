package mrtdma;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import mrtdma.Records.*;
import mrtdma.Step1.*;
import mrtdma.Step2.*;
import mrtdma.Step3.*;

/** Three Hadoop jobs per ADI direction. Each task holds only a tile/partition. */
public class Driver extends Configured implements Tool {
    static final String COUNT="MR_TDMA";
    static final class Geometry {
        final Grid grid;
        final int b,axis,length,parts,tilesX,tilesY,tilesZ,lineWidth,lineHeight;
        Geometry(Configuration c) {
            this(new Grid(c),c.getInt("tdma.block",0),c.getInt("tdma.axis",2));
        }
        Geometry(Grid grid,int block,int axis) {
            this.grid=grid; b=block; this.axis=axis;
            tilesX=(grid.nx-1)/b+1; tilesY=(grid.ny-1)/b+1; tilesZ=(grid.nz-1)/b+1;
            length=axis==2?grid.nz:axis==1?grid.ny:grid.nx;
            parts=(length-1)/b+1;
            lineWidth=axis==0?grid.ny:grid.nx;
            lineHeight=axis==2?grid.ny:grid.nz;
        }
        long tiles() { return (long)tilesX*tilesY*tilesZ; }
        long lines() { return (long)lineWidth*lineHeight; }
        long partitions() { return lines()*parts; }
        int sx(int tx) { return Math.min(b,grid.nx-tx*b); }
        int sy(int ty) { return Math.min(b,grid.ny-ty*b); }
        int sz(int tz) { return Math.min(b,grid.nz-tz*b); }
        int[] tile(long id) { return new int[]{(int)(id/(tilesY*(long)tilesZ)),(int)(id/tilesZ%tilesY),(int)(id%tilesZ)}; }
        long tileId(int x,int y,int z) { return ((x/b)*(long)tilesY+y/b)*tilesZ+z/b; }
        long line(int x,int y,int z) { return axis==2?x*(long)lineHeight+y:axis==1?x*(long)lineHeight+z:y*(long)lineHeight+z; }
        int row(int x,int y,int z) { return axis==2?z:axis==1?y:x; }
        int[] point(PartitionKey k) {
            int p=(int)(k.line/lineHeight),q=(int)(k.line%lineHeight),r=k.part*b;
            return axis==2?new int[]{p,q,r}:axis==1?new int[]{p,r,q}:new int[]{r,p,q};
        }
    }
    interface CoefficientSink { void write(PartitionKey key,Coefficients value) throws IOException,InterruptedException; }
    /** Input preparation is materialized before Step 1, not performed by its Mapper. */
    static void prepareTile(Geometry g,Numerics.Physics physics,long tileId,double[] values,boolean initial,CoefficientSink sink) throws IOException,InterruptedException {
        int[] t=g.tile(tileId); int nx=g.sx(t[0]),ny=g.sy(t[1]),nz=g.sz(t[2]);
        if(!initial&&values.length!=nx*ny*nz) throw new IOException("Incomplete input tile "+tileId);
        int length=g.axis==2?nz:g.axis==1?ny:nx;
        Coefficients v=new Coefficients(length);
        int outer1=g.axis==0?ny:nx,outer2=g.axis==2?ny:nz;
        for(int p=0;p<outer1;p++) for(int q=0;q<outer2;q++) {
            int x=g.axis==0?0:p, y=g.axis==2?q:g.axis==1?0:p, z=g.axis==2?0:q;
            int gx=t[0]*g.b+x,gy=t[1]*g.b+y,gz=t[2]*g.b+z;
            int offset=(x*ny+y)*nz+z,stride=g.axis==2?1:g.axis==1?nz:ny*nz;
            int first=g.row(gx,gy,gz);
            long componentClock=TaskTiming.start();
            for(int i=0;i<length;i++) {
                physics.coefficients(g.axis,first+i,v.a,v.b,v.c,i);
                v.d[i]=initial?physics.initialRhs(gx,gy,gz+i):values[offset+i*stride];
                if(!Double.isFinite(v.d[i])) throw new IOException("Non-finite RHS");
            }
            TaskTiming.computed(componentClock);
            sink.write(new PartitionKey(g.line(gx,gy,gz),first/g.b),v);
        }
    }
    /** Broadcast all boundary MapFiles on local disk, not as a global JVM array. */
    private static Map<String,Object> cacheBoundaries(Job job,FileSystem fs,Path source,Configuration conf) throws IOException {
        long start=System.nanoTime(),raw=0; int files=0;
        Path archive=new Path(source.getParent(),"boundaries-distributed-cache.zip");
        byte[] buffer=new byte[1024*1024];
        try(ZipOutputStream zip=new ZipOutputStream(fs.create(archive,false))) {
            zip.setLevel(1);
            FileStatus[] dirs=fs.globStatus(new Path(source,"part-m-*"));
            if(dirs==null||dirs.length==0) throw new IOException("No boundary MapFiles");
            for(FileStatus dir:dirs) for(String name:new String[]{"data","index"}) {
                Path file=new Path(dir.getPath(),name); long bytes=fs.getFileStatus(file).getLen();
                zip.putNextEntry(new ZipEntry(dir.getPath().getName()+"/"+name));
                try(FSDataInputStream input=fs.open(file)) {
                    int read; while((read=input.read(buffer))!=-1) zip.write(buffer,0,read);
                }
                zip.closeEntry(); raw+=bytes; files++;
            }
        }
        job.addCacheArchive(URI.create(fs.makeQualified(archive).toString()+"#tdma-boundaries"));
        job.getConfiguration().set("tdma.boundary.transport","DistributedCache archive; local MapFile lookup");
        job.getConfiguration().set("tdma.reconstruction.location","REDUCE");
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("archive",archive.toString()); result.put("rawBytes",raw); result.put("archiveBytes",fs.getFileStatus(archive).getLen());
        result.put("files",files); result.put("preparationSeconds",(System.nanoTime()-start)/1e9);
        return result;
    }

    private static void output(Job job,Class<?> format,Class<?> key,Class<?> value,Path path) throws IOException {
        job.setOutputFormatClass((Class)format); job.setOutputKeyClass(key); job.setOutputValueClass(value);
        FileOutputFormat.setOutputPath(job,path);
        FileOutputFormat.setCompressOutput(job,true); FileOutputFormat.setOutputCompressorClass(job,DefaultCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job,SequenceFile.CompressionType.BLOCK);
    }
    private Job job(Configuration conf,String name,int reducers) throws IOException {
        Job job=Job.getInstance(conf,name); job.setJarByClass(Driver.class); job.setNumReduceTasks(reducers);
        job.getConfiguration().setBoolean("mapreduce.map.speculative",false);
        job.getConfiguration().setBoolean("mapreduce.reduce.speculative",false);
        job.getConfiguration().setBoolean("mapreduce.job.ubertask.enable",false);
        job.getConfiguration().setBoolean("mapreduce.map.output.compress",true);
        job.getConfiguration().set("mapreduce.map.output.compress.codec",DefaultCodec.class.getName());
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize",128L*1024*1024);
        job.getConfiguration().setLong("mapreduce.task.timeout",900000);
        return job;
    }
    private static BufferedReader text(FileSystem fs,Path path,Configuration conf) throws IOException {
        java.io.InputStream stream=fs.open(path);
        CompressionCodec codec=new CompressionCodecFactory(conf).getCodec(path);
        if(codec!=null) stream=codec.createInputStream(stream);
        return new BufferedReader(new InputStreamReader(stream,StandardCharsets.UTF_8));
    }
    private void execute(Job job,List<Map<String,Object>> records,long expected,String counter) throws Exception {
        long start=System.nanoTime();
        if(!job.waitForCompletion(true)) throw new IOException("Failed "+job.getJobName()+" "+job.getJobID());
        long actual=job.getCounters().findCounter(COUNT,counter).getValue();
        if(actual!=expected) throw new IOException(counter+"="+actual+" expected="+expected);
        int expectedMaps=job.getConfiguration().getInt("tdma.maps",-1);
        long launchedMaps=job.getCounters().findCounter(org.apache.hadoop.mapreduce.JobCounter.TOTAL_LAUNCHED_MAPS).getValue();
        long nonemptyMaps=job.getCounters().findCounter(COUNT,"NONEMPTY_MAPS").getValue();
        // Retries increase launched attempts; verify coverage using successful task counters.
        if(nonemptyMaps!=expectedMaps)
            throw new IOException("Map coverage nonempty="+nonemptyMaps+" expected="+expectedMaps);
        Map<String,Object> record=new LinkedHashMap<>();
        record.put("name",job.getJobName()); record.put("jobId",job.getJobID().toString());
        record.put("reduceTasks",job.getNumReduceTasks()); record.put("verifiedCounter",counter); record.put("verifiedCount",actual);
        record.put("mapTasks",expectedMaps); record.put("nonemptyMaps",nonemptyMaps);
        record.put("launchedMapAttempts",launchedMaps);
        record.put("elapsedSeconds",(System.nanoTime()-start)/1e9); records.add(record);
        System.out.println("STAGE_VERIFIED "+new ObjectMapper().writeValueAsString(record));
    }
    /** The plan and the executor use the same input-format selection. */
    static Class<? extends org.apache.hadoop.mapreduce.InputFormat> step1InputFormat(boolean spatial) {
        return spatial?FixedSpatialInputs.class:Inputs.WholeSequenceFiles.class;
    }
    public int run(String[] args) throws Exception {
        boolean planOnly=args.length>0&&args[0].equals("--plan");
        if(planOnly)args=java.util.Arrays.copyOfRange(args,1,args.length);
        Grid grid=new Grid(args,5,"N or Nx Ny Nz, then block reducers maps output-path prepared-initial-coefficients");
        int offset=args.length-5;
        int b=Integer.parseInt(args[offset]),reducers=Integer.parseInt(args[offset+1]),maps=Integer.parseInt(args[offset+2]);
        grid.validate(b);
        if(reducers<1||maps<1) throw new IllegalArgumentException("Need positive Map and Reduce counts");
        Path root=new Path(args[offset+3]),input=new Path(args[offset+4]);
        Configuration conf=new JobConf(getConf());
        grid.configure(conf,b); conf.setInt("tdma.maps",maps);
        int spatialWidth=conf.getInt("tdma.slice.width",0),spatialHeight=conf.getInt("tdma.slice.height",0);
        boolean spatial=spatialWidth>0||spatialHeight>0;
        String assignmentFlag=conf.get("tdma.spatial.fixed.maps");
        if(assignmentFlag!=null&&!conf.getBoolean("tdma.spatial.fixed.maps",true))
            throw new IllegalArgumentException("Only fixed-count spatial assignment is supported; region-count mode is not available");
        if(assignmentFlag!=null&&!spatial)throw new IllegalArgumentException("Spatial assignment requires input-region dimensions");
        if(spatial && (spatialWidth<1||spatialHeight<1||spatialWidth>187||spatialHeight>187||maps!=reducers))
            throw new IllegalArgumentException("Spatial input requires valid dimensions and equal explicit Map/Reduce counts");
        conf.setBoolean("tdma.spatial.input",spatial);
        long points=grid.points();
        Geometry z=new Geometry(grid,b,2),y=new Geometry(grid,b,1),x=new Geometry(grid,b,0);
        String gridLabel=grid.cube()?Integer.toString(grid.nx):grid.label();
        Object size=grid.cube()?(Object)grid.nx:grid.label();
        if(planOnly) {
            Map<String,Object> plan=new LinkedHashMap<>();
            plan.put("sourceRelease","rectangular-20261103");
            plan.put("grid",size);plan.put("dimensions",grid.dimensions());
            plan.put("partitionsPerLineZYX",new int[]{z.parts,y.parts,x.parts});plan.put("subsystemLength",b);
            plan.put("inputAssignment",spatial?"fixed-count spatial":"explicit-count coefficient files");
            plan.put("step1InputFormat",step1InputFormat(spatial).getName());
            plan.put("requestedMapsPerDirection",new int[]{maps,spatial?reducers:maps,spatial?reducers:maps});
            plan.put("requestedReducersPerDirection",new int[]{reducers,0,reducers});
            plan.put("directions",new String[]{"Z","Y","X"});plan.put("solutionJobs",9);
            plan.put("mapMemoryMiB",conf.getInt("mapreduce.map.memory.mb",-1));
            plan.put("mapJavaOptions",conf.get("mapreduce.map.java.opts","cluster default"));
            plan.put("runningMapLimit",conf.getInt("mapreduce.job.running.map.limit",-1));
            if(spatial){plan.put("regionWidth",spatialWidth);plan.put("regionHeight",spatialHeight);}
            plan.put("output",root.toString());plan.put("preparedInput",input.toString());
            plan.put("submittedJobs",0);plan.put("filesystemAccess",false);
            plan.put("note","Requested configuration only; active workers, actual splits, resource admission and completed execution require cluster evidence.");
            System.out.println("MR_TDMA_PLAN "+new ObjectMapper().writeValueAsString(plan));return 0;
        }
        FileSystem fs=root.getFileSystem(conf);
        if(fs.exists(root))throw new IOException("Output already exists: "+root);
        fs.mkdirs(root);
        List<Map<String,Object>> records=new ArrayList<>(),caches=new ArrayList<>(); Path previous=null;
        long pipelineStart=System.nanoTime();
        for(int axis=2;axis>=0;axis--) {
            String name=new String[]{"X","Y","Z"}[axis]; Configuration current=new Configuration(conf);
            Geometry geometry=new Geometry(grid,b,axis);
            long partitions=geometry.partitions();
            current.setInt("tdma.axis",axis); current.setBoolean("tdma.initial",axis==2);
            Path first=new Path(root,name+"/step1"),second=new Path(root,name+"/step2"),third=new Path(root,name+"/step3");
            Job step1=job(current,"MR-TDMA "+gridLabel+" "+name+" Step1 Map-Reduce local reduction",reducers);
            step1.setMapperClass(PartitionMapper.class); step1.setReducerClass(LocalReduction.class); step1.setPartitionerClass(LinePartitioner.class);
            step1.setMapOutputKeyClass(PartitionKey.class); step1.setMapOutputValueClass(Coefficients.class);
            step1.setInputFormatClass(step1InputFormat(spatial));
            FileInputFormat.addInputPath(step1,spatial?(axis==2?input:new Path(previous,"coefficients")):(axis==2?new Path(input,"part-*"):new Path(previous,"coefficients/part-r-*")));
            output(step1,MapFileOutputFormat.class,PartitionKey.class,Boundary.class,first);
            MultipleOutputs.addNamedOutput(step1,"state",SequenceFileOutputFormat.class,PartitionKey.class,State.class);
            MultipleOutputs.addNamedOutput(step1,"placement",TextOutputFormat.class,Text.class,Text.class);
            execute(step1,records,partitions,"PARTITIONS_REDUCED");

            Job step2=job(current,"MR-TDMA "+gridLabel+" "+name+" Step2 Map-only reduced solve",0);
            if(spatial) step2.getConfiguration().setInt("tdma.maps",reducers);
            step2.setMapperClass(ReducedSolver.class); step2.setInputFormatClass(DistributedBoundaryInput.class);
            step2.getConfiguration().setInt("tdma.distributed.shards",reducers);
            step2.getConfiguration().set("tdma.distributed.boundaries",first.toString());
            output(step2,MapFileOutputFormat.class,PartitionKey.class,Ends.class,second);
            MultipleOutputs.addNamedOutput(step2,"shard",TextOutputFormat.class,Text.class,Text.class);
            MultipleOutputs.addNamedOutput(step2,"placement",TextOutputFormat.class,Text.class,Text.class);
            execute(step2,records,geometry.lines(),"REDUCED_LINES_SOLVED");

            Configuration reconstruction=new Configuration(current); reconstruction.set("tdma.interfaces",second.toString());
            BitSet mappedShards=new BitSet(reducers);
            for(FileStatus status:fs.globStatus(new Path(second,"shards/part-m-*"))) {
                try(BufferedReader reader=text(fs,status.getPath(),conf)) {
                    String line; while((line=reader.readLine())!=null) {
                        String[] fields=line.split("\t"); int shard=Integer.parseInt(fields[0]);
                        if(fields.length!=2||shard<0||shard>=reducers||mappedShards.get(shard)) throw new IOException("Invalid interface shard mapping");
                        mappedShards.set(shard); reconstruction.set("tdma.interface.file."+shard,fields[1]);
                    }
                }
            }
            if(mappedShards.cardinality()!=reducers) throw new IOException("Incomplete interface shard mapping");
            Job step3=job(reconstruction,"MR-TDMA "+gridLabel+" "+name+" Step3 Map-attach Reduce-reconstruct",reducers);
            if(spatial) step3.getConfiguration().setInt("tdma.maps",reducers);
            step3.setMapperClass(AttachBoundaries.class); step3.setReducerClass(ReconstructAndAssemble.class);
            step3.setPartitionerClass(TilePartitioner.class);
            step3.setInputFormatClass(Inputs.WholeSequenceFiles.class); FileInputFormat.addInputPath(step3,new Path(first,"state/part-r-*"));
            step3.setMapOutputKeyClass(ReconstructionKey.class); step3.setMapOutputValueClass(Update.class);
            output(step3,MapFileOutputFormat.class,LongWritable.class,Tile.class,third);
            MultipleOutputs.addNamedOutput(step3,"placement",TextOutputFormat.class,Text.class,Text.class);
            MultipleOutputs.addNamedOutput(step3,"coefficients",SequenceFileOutputFormat.class,PartitionKey.class,Coefficients.class);
            Map<String,Object> cache=cacheBoundaries(step3,fs,second,current); cache.put("axis",name); caches.add(cache);
            System.out.println("CACHE_BROADCAST "+new ObjectMapper().writeValueAsString(cache));
            execute(step3,records,points,"POINTS_ASSEMBLED");
            if(spatial && axis>0) {
                long written=step3.getCounters().findCounter(SpatialCoefficientOutputs.COUNTERS,"COEFFICIENT_RECORDS_WRITTEN").getValue();
                long expectedNext=new Geometry(grid,b,axis-1).partitions();
                if(written!=expectedNext) throw new IOException("Spatial coefficient output coverage "+written+"/"+expectedNext);
                Map<String,Object> io=records.get(records.size()-1);
                io.put("coefficientOutputRecords",written);
                io.put("coefficientOutputFiles",step3.getCounters().findCounter(SpatialCoefficientOutputs.COUNTERS,"OUTPUT_FILES_CREATED").getValue());
                io.put("coefficientWriterEvictions",step3.getCounters().findCounter(SpatialCoefficientOutputs.COUNTERS,"WRITER_EVICTIONS").getValue());
                io.put("maxOpenCoefficientWritersPerReducer",SpatialCoefficientOutputs.MAX_OPEN_WRITERS);
            }
            if(step3.getCounters().findCounter(COUNT,"POINTS_RECONSTRUCTED_REDUCE").getValue()!=points||
               step3.getCounters().findCounter(COUNT,"PARTITIONS_RECONSTRUCTED_REDUCE").getValue()!=partitions||
               step3.getCounters().findCounter(COUNT,"CACHE_PARTITIONS_ATTACHED").getValue()!=partitions)
                throw new IOException("Reduce reconstruction / cache attachment coverage mismatch");
            previous=third;
        }
        Map<String,Object> summary=new LinkedHashMap<>(); summary.put("size",size); summary.put("dimensions",grid.dimensions()); summary.put("interiorPoints",points);
        summary.put("blockEdge",b); if(grid.cube())summary.put("partitionsPerLine",z.parts);
        summary.put("partitionsPerLineZYX",new int[]{z.parts,y.parts,x.parts}); summary.put("reducers",reducers); summary.put("jobs",records);
        summary.put("finalField",previous.toString()); summary.put("timeSteps",1);
        summary.put("implementation","distributed-partitions-reduce-local-v1");
        summary.put("sourceRelease","rectangular-20261103");
        summary.put("boundaryGathering",reducers+"-way ordered merge of boundary MapFiles in Step2 RecordReader; no numerical solve or coefficient transformation in reader");
        if(spatial) { summary.put("sliceWidth",spatialWidth); summary.put("sliceHeight",spatialHeight); }
        if(spatial) { summary.put("inputAssignment","fixed-map-count spatial file assignment"); summary.put("spatialRegions",SpatialInputs.regionCount(z,spatialWidth,spatialHeight)); }
        if(spatial) { summary.put("step1Maps",maps); summary.put("step2Maps",reducers); summary.put("step3Maps",reducers); }
        else summary.put("mapsPerJob",maps);
        summary.put("boundaryTransport","DistributedCache archive localized on each executing worker; no global boundary heap map");
        summary.put("step1MapRole","pass-through prepared coefficient records"); summary.put("initialCoefficients",input.toString());
        summary.put("nextDirectionCoefficientPreparation","previous direction Step3 Reduce, one bounded tile at a time");
        summary.put("reconstructionLocation","REDUCE"); summary.put("distributedCaches",caches);
        summary.put("pipelineElapsedSeconds",(System.nanoTime()-pipelineStart)/1e9);
        try(java.io.OutputStream out=fs.create(new Path(root,"run-summary.json"),false)) { new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(out,summary); }
        System.out.println("MR_TDMA_RESULT "+new ObjectMapper().writeValueAsString(summary));
        return 0;
    }
    public static void main(String[] args) throws Exception { System.exit(ToolRunner.run(new JobConf(),new Driver(),args)); }
}
