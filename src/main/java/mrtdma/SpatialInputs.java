package mrtdma;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/** Spatial geometry and validated file inventory for fixed-count assignment. */
public final class SpatialInputs {
    private SpatialInputs() {}
    static int regionCount(Driver.Geometry g,int w,int h){
        return Math.multiplyExact((g.lineWidth-1)/w+1,(g.lineHeight-1)/h+1);
    }
    static int regionId(long line,Driver.Geometry g,int w,int h){
        return (int)(line/g.lineHeight)/w*((g.lineHeight-1)/h+1)+(int)(line%g.lineHeight)/h;
    }
    static String[] prefixes(Driver.Geometry g,int w,int h,String parent){
        String[] result=new String[regionCount(g,w,h)];
        for(int i=0;i<result.length;i++)result[i]=parent+String.format(Locale.ROOT,"region-%05d/part",i);
        return result;
    }
    static final class RegionFiles {
        final int region;final Path[] paths;final long[] lengths;
        RegionFiles(int region,List<FileStatus> files){
            this.region=region;paths=new Path[files.size()];lengths=new long[paths.length];
            for(int i=0;i<paths.length;i++){paths[i]=files.get(i).getPath();lengths[i]=files.get(i).getLen();}
        }
    }
    static List<RegionFiles> inventory(JobContext context)throws IOException{
        Configuration conf=context.getConfiguration();
        Driver.Geometry g=new Driver.Geometry(conf);
        int w=conf.getInt("tdma.slice.width",0),h=conf.getInt("tdma.slice.height",0);
        int count=regionCount(g,w,h);Map<Integer,List<FileStatus>> groups=new TreeMap<>();
        for(Path root:FileInputFormat.getInputPaths(context)){
            FileSystem fs=root.getFileSystem(conf);FileStatus[] regions=fs.globStatus(new Path(root,"region-*"));
            if(regions==null)throw new IOException("No spatial region directories: "+root);
            for(FileStatus dir:regions){
                if(!dir.isDirectory())throw new IOException("Invalid region directory");
                int id=Integer.parseInt(dir.getPath().getName().substring("region-".length()));
                if(id<0||id>=count)throw new IOException("Invalid region ID "+id);
                List<FileStatus> files=groups.computeIfAbsent(id,k->new ArrayList<>());
                FileStatus[] parts=fs.globStatus(new Path(dir.getPath(),"part-*"));
                if(parts!=null)for(FileStatus file:parts){
                    if(!file.isFile()||file.getLen()==0)throw new IOException("Invalid spatial input file");
                    files.add(file);
                }
            }
        }
        if(groups.size()!=count)throw new IOException("Missing spatial regions "+groups.size()+"/"+count);
        List<RegionFiles> result=new ArrayList<>();
        for(int id=0;id<count;id++){
            List<FileStatus> files=groups.get(id);
            if(files==null||files.isEmpty())throw new IOException("Empty spatial region "+id);
            files.sort(Comparator.comparing(f->f.getPath().toString()));
            result.add(new RegionFiles(id,files));
        }
        return result;
    }
}
