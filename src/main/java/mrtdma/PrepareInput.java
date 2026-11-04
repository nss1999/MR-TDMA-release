package mrtdma;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.DefaultCodec;
import mrtdma.Records.*;

/** Materialize initial tridiagonal input before the paper's three-job workflow. */
public final class PrepareInput {
    public static void main(String[] args) throws Exception {
        Grid grid=new Grid(args,3,"N or Nx Ny Nz, then block writers new-HDFS-directory");
        int offset=args.length-3;
        int block=Integer.parseInt(args[offset]),writers=Integer.parseInt(args[offset+1]);
        grid.validate(block);
        if(writers<1||writers>4) throw new IllegalArgumentException("Need 1..4 writers");
        Configuration conf=new Configuration();
        Driver.Geometry g=new Driver.Geometry(grid,block,2);
        Path output=new Path(args[offset+2]); FileSystem fs=output.getFileSystem(conf);
        if(fs.exists(output)) throw new IllegalArgumentException("Output exists: "+output);
        fs.mkdirs(output); long total=g.tiles();
        ExecutorService pool=Executors.newFixedThreadPool(writers); List<Callable<Long>> jobs=new ArrayList<>();
        for(int shard=0;shard<writers;shard++) {
            final int id=shard;
            jobs.add(()->{
                Numerics.Physics physics=new Numerics.Physics(grid.nx,grid.ny,grid.nz);
                long[] count={0};
                DefaultCodec codec=new DefaultCodec(); codec.setConf(conf);
                try(SequenceFile.Writer writer=SequenceFile.createWriter(conf,
                        SequenceFile.Writer.file(new Path(output,String.format("part-%05d",id))),
                        SequenceFile.Writer.keyClass(PartitionKey.class),SequenceFile.Writer.valueClass(Coefficients.class),
                        SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK,codec))) {
                    for(long tile=total*id/writers;tile<total*(id+1)/writers;tile++) {
                        PartitionedTDMA.prepareTile(g,physics,tile,new double[0],true,(key,value)->{writer.append(key,value); count[0]++;});
                    }
                }
                return count[0];
            });
        }
        try {
            long partitions=0; for(Future<Long> future:pool.invokeAll(jobs)) partitions+=future.get();
            if(partitions!=g.partitions()) throw new IllegalStateException("Incomplete coefficient preparation");
            try(java.io.OutputStream complete=fs.create(new Path(output,"_SUCCESS"),false)) {}
            System.out.println("PREPARED_COEFFICIENT_PARTITIONS="+partitions);
        } finally { pool.shutdownNow(); }
    }
}
