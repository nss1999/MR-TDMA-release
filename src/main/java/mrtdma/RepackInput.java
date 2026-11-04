package mrtdma;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.security.MessageDigest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.DefaultCodec;
import mrtdma.Records.*;

/** Repackage unchanged coefficient records; verify their complete serialized sequence. */
public final class RepackInput {
    private static void digest(MessageDigest hash,DataOutputBuffer buffer,PartitionKey key,Coefficients value) throws Exception {
        buffer.reset();key.write(buffer);value.write(buffer);
        hash.update(buffer.getData(),0,buffer.getLength());
    }
    private static String hex(byte[] bytes) {
        StringBuilder out=new StringBuilder();for(byte b:bytes)out.append(String.format("%02x",b&255));return out.toString();
    }
    public static void main(String[] args) throws Exception {
        if(args.length!=5)throw new IllegalArgumentException("N block output-files source-directory new-directory");
        int n=Integer.parseInt(args[0]),b=Integer.parseInt(args[1]),count=Integer.parseInt(args[2]);
        if(n<4||b<2||n%b!=0||count<1||count>480)throw new IllegalArgumentException("Need divisible geometry and 1..480 output files");
        Configuration conf=new Configuration();Path source=new Path(args[3]),dest=new Path(args[4]);
        FileSystem fs=dest.getFileSystem(conf);if(fs.exists(dest))throw new IllegalArgumentException("Output exists: "+dest);
        FileStatus[] inputs=fs.globStatus(new Path(source,"part-*"));
        if(inputs==null||inputs.length==0)throw new IllegalArgumentException("No original input files");
        Arrays.sort(inputs,(a,c)->a.getPath().toString().compareTo(c.getPath().toString()));fs.mkdirs(dest);
        SequenceFile.Writer[] writers=new SequenceFile.Writer[count];long[] perFile=new long[count];
        MessageDigest original=MessageDigest.getInstance("SHA-256");DataOutputBuffer buffer=new DataOutputBuffer();long records=0;
        PartitionKey key=new PartitionKey();Coefficients value=new Coefficients();
        try {
            for(int i=0;i<count;i++) {
                DefaultCodec codec=new DefaultCodec();codec.setConf(conf);
                writers[i]=SequenceFile.createWriter(conf,SequenceFile.Writer.file(new Path(dest,String.format("part-%05d",i))),
                    SequenceFile.Writer.keyClass(PartitionKey.class),SequenceFile.Writer.valueClass(Coefficients.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK,codec));
            }
            for(FileStatus file:inputs)try(SequenceFile.Reader reader=new SequenceFile.Reader(conf,SequenceFile.Reader.file(file.getPath()))) {
                while(reader.next(key,value)) {
                    if(value.n!=b||key.line<0||key.line>=(long)n*n||key.part<0||key.part>=n/b)
                        throw new IllegalStateException("Invalid original coefficient record");
                    int shard=(int)(records%count);writers[shard].append(key,value);perFile[shard]++;
                    digest(original,buffer,key,value);records++;
                }
            }
        } finally {for(SequenceFile.Writer writer:writers)if(writer!=null)writer.close();}
        long expected=(long)n*n*(n/b);if(records!=expected)throw new IllegalStateException("Original coverage mismatch");
        SequenceFile.Reader[] readers=new SequenceFile.Reader[count];MessageDigest replay=MessageDigest.getInstance("SHA-256");
        try {
            for(int i=0;i<count;i++)readers[i]=new SequenceFile.Reader(conf,SequenceFile.Reader.file(new Path(dest,String.format("part-%05d",i))));
            for(long r=0;r<records;r++) {
                if(!readers[(int)(r%count)].next(key,value))throw new IllegalStateException("Missing repacked record");
                digest(replay,buffer,key,value);
            }
            for(int i=0;i<count;i++)if(perFile[i]==0||readers[i].next(key,value))throw new IllegalStateException("Empty or extra repacked records");
        } finally {for(SequenceFile.Reader reader:readers)if(reader!=null)reader.close();}
        String before=hex(original.digest()),after=hex(replay.digest());
        if(!before.equals(after))throw new IllegalStateException("Repack changed coefficient records");
        Map<String,Object> result=new LinkedHashMap<>();result.put("source",source.toString());result.put("output",dest.toString());
        result.put("size",n);result.put("blockEdge",b);result.put("files",count);result.put("records",records);
        result.put("recordsPerFile",perFile);result.put("sourceRecordSequenceSha256",before);result.put("replayedRecordSequenceSha256",after);
        result.put("coefficientRecordsBitwiseUnchanged",true);
        try(java.io.OutputStream out=fs.create(new Path(dest,"_repack-manifest.json"),false)){new ObjectMapper().writeValue(out,result);}
        try(java.io.OutputStream out=fs.create(new Path(dest,"_SUCCESS"),false)){}
        System.out.println("REPACK_VERIFIED "+new ObjectMapper().writeValueAsString(result));
    }
}
