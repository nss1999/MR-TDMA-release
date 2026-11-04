package mrtdma;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;

/** Same DFS client; clocks surround synchronous file API calls only.
 * Background DataNode service time and local files are not measured here. */
public final class TimedDistributedFileSystem extends DistributedFileSystem {
    @Override public FSDataInputStream open(Path path,int bufferSize) throws IOException {
        long t=System.nanoTime();FSDataInputStream stream;
        try { stream=super.open(path,bufferSize); } finally { TaskTiming.io(t,0,0); }
        return new FSDataInputStream(new TimedInput(stream));
    }
    @Override public FSDataOutputStream create(Path path,FsPermission permission,boolean overwrite,int bufferSize,short replication,long blockSize,Progressable progress) throws IOException {
        long t=System.nanoTime();FSDataOutputStream stream;
        try { stream=super.create(path,permission,overwrite,bufferSize,replication,blockSize,progress); }
        finally { TaskTiming.io(t,0,0); }
        // The underlying DFS stream owns the standard byte statistics. Do not
        // count those bytes twice in this transparent outer stream.
        return new FSDataOutputStream(new TimedOutput(stream),null,stream.getPos());
    }
    private static final class TimedInput extends FSInputStream implements ByteBufferReadable,CanSetDropBehind,CanSetReadahead,StreamCapabilities,IOStatisticsSource {
        private final FSDataInputStream in;
        TimedInput(FSDataInputStream in) { this.in=in; }
        @Override public int read() throws IOException {
            long t=System.nanoTime();int n=-1;try { n=in.read();return n; } finally { TaskTiming.io(t,n<0?0:1,0); }
        }
        @Override public int read(byte[] b,int offset,int length) throws IOException {
            long t=System.nanoTime();int n=0;try { n=in.read(b,offset,length);return n; } finally { TaskTiming.io(t,Math.max(0,n),0); }
        }
        @Override public int read(long position,byte[] b,int offset,int length) throws IOException {
            long t=System.nanoTime();int n=0;try { n=in.read(position,b,offset,length);return n; } finally { TaskTiming.io(t,Math.max(0,n),0); }
        }
        @Override public void readFully(long position,byte[] b,int offset,int length) throws IOException {
            long t=System.nanoTime();int n=0;try { in.readFully(position,b,offset,length);n=length; } finally { TaskTiming.io(t,n,0); }
        }
        @Override public int read(ByteBuffer b) throws IOException {
            long t=System.nanoTime();int n=0;try { n=in.read(b);return n; } finally { TaskTiming.io(t,Math.max(0,n),0); }
        }
        @Override public void seek(long position) throws IOException { long t=System.nanoTime();try { in.seek(position); } finally { TaskTiming.io(t,0,0); } }
        @Override public boolean seekToNewSource(long position) throws IOException { long t=System.nanoTime();try { return in.seekToNewSource(position); } finally { TaskTiming.io(t,0,0); } }
        @Override public long getPos() throws IOException { return in.getPos(); }
        @Override public int available() throws IOException { return in.available(); }
        @Override public void close() throws IOException { long t=System.nanoTime();try { in.close(); } finally { TaskTiming.io(t,0,0); } }
        @Override public void setDropBehind(Boolean value) throws IOException { in.setDropBehind(value); }
        @Override public void setReadahead(Long value) throws IOException { in.setReadahead(value); }
        @Override public boolean hasCapability(String capability) { return in.hasCapability(capability); }
        @Override public IOStatistics getIOStatistics() { return in.getIOStatistics(); }
    }
    private static final class TimedOutput extends OutputStream implements Syncable,CanSetDropBehind,StreamCapabilities,IOStatisticsSource {
        private final FSDataOutputStream out;
        TimedOutput(FSDataOutputStream out) { this.out=out; }
        @Override public void write(int value) throws IOException { long t=System.nanoTime();int n=0;try { out.write(value);n=1; } finally { TaskTiming.io(t,0,n); } }
        @Override public void write(byte[] b,int offset,int length) throws IOException { long t=System.nanoTime();int n=0;try { out.write(b,offset,length);n=length; } finally { TaskTiming.io(t,0,n); } }
        @Override public void flush() throws IOException { long t=System.nanoTime();try { out.flush(); } finally { TaskTiming.io(t,0,0); } }
        @Override public void hflush() throws IOException { long t=System.nanoTime();try { out.hflush(); } finally { TaskTiming.io(t,0,0); } }
        @Override public void hsync() throws IOException { long t=System.nanoTime();try { out.hsync(); } finally { TaskTiming.io(t,0,0); } }
        @Override public void close() throws IOException { long t=System.nanoTime();try { out.close(); } finally { TaskTiming.io(t,0,0); } }
        @Override public void setDropBehind(Boolean value) throws IOException { out.setDropBehind(value); }
        @Override public boolean hasCapability(String capability) { return out.hasCapability(capability); }
        @Override public IOStatistics getIOStatistics() { return out.getIOStatistics(); }
    }
}
