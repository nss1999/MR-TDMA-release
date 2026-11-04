package mrtdma;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public final class Records {
    private Records() {}
    private static int length(DataInput in, int maximum) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > maximum) throw new IOException("Invalid array length " + n);
        return n;
    }
    public static final class PartitionKey implements WritableComparable<PartitionKey> {
        public long line;
        public int part;
        public PartitionKey() {}
        public PartitionKey(long line, int part) { this.line=line; this.part=part; }
        public void write(DataOutput out) throws IOException { out.writeLong(line); out.writeInt(part); }
        public void readFields(DataInput in) throws IOException { line=in.readLong(); part=in.readInt(); }
        public int compareTo(PartitionKey other) { int c=Long.compare(line,other.line); return c!=0?c:Integer.compare(part,other.part); }
        public int hashCode() { return Long.hashCode(line)*31+part; }
        public boolean equals(Object o) { return o instanceof PartitionKey && compareTo((PartitionKey)o)==0; }
        public String toString() { return line+":"+part; }
    }
    public static final class Coefficients implements Writable {
        public int n;
        public double[] a,b,c,d;
        public Coefficients() { this(0); }
        public Coefficients(int n) { resize(n); }
        public void resize(int size) {
            n=size;
            if(a==null||a.length!=n) { a=new double[n]; b=new double[n]; c=new double[n]; d=new double[n]; }
        }
        public void write(DataOutput out) throws IOException {
            out.writeInt(n);
            for(int i=0;i<n;i++) { out.writeDouble(a[i]); out.writeDouble(b[i]); out.writeDouble(c[i]); out.writeDouble(d[i]); }
        }
        public void readFields(DataInput in) throws IOException {
            resize(length(in,256));
            for(int i=0;i<n;i++) { a[i]=in.readDouble(); b[i]=in.readDouble(); c[i]=in.readDouble(); d[i]=in.readDouble(); }
        }
    }
    public static final class State implements Writable {
        public int n;
        public double[] a,c,d;
        public State() { resize(0); }
        public State(Coefficients v) { n=v.n; a=v.a; c=v.c; d=v.d; }
        public void resize(int size) {
            n=size; if(a==null||a.length!=n) { a=new double[n]; c=new double[n]; d=new double[n]; }
        }
        public void write(DataOutput out) throws IOException {
            out.writeInt(n); for(int i=0;i<n;i++) { out.writeDouble(a[i]); out.writeDouble(c[i]); out.writeDouble(d[i]); }
        }
        public void readFields(DataInput in) throws IOException {
            resize(length(in,256)); for(int i=0;i<n;i++) { a[i]=in.readDouble(); c[i]=in.readDouble(); d[i]=in.readDouble(); }
        }
    }
    public static final class Boundary implements Writable {
        public final double[] v=new double[6];
        public Boundary() {}
        public Boundary(Coefficients s) {
            int last=s.n-1;
            v[0]=s.a[0]; v[1]=s.c[0]; v[2]=s.d[0];
            v[3]=s.a[last]; v[4]=s.c[last]; v[5]=s.d[last];
        }
        public void write(DataOutput out) throws IOException { for(double x:v) out.writeDouble(x); }
        public void readFields(DataInput in) throws IOException { for(int i=0;i<6;i++) v[i]=in.readDouble(); }
    }
    public static final class Bundle implements Writable {
        public double[] rows=new double[0];
        public void write(DataOutput out) throws IOException { out.writeInt(rows.length); for(double x:rows) out.writeDouble(x); }
        public void readFields(DataInput in) throws IOException { int n=length(in,6*1048576); rows=new double[n]; for(int i=0;i<n;i++) rows[i]=in.readDouble(); }
    }
    public static final class Ends implements Writable {
        public double first,last;
        public Ends() {}
        public Ends(double first,double last) { this.first=first; this.last=last; }
        public void write(DataOutput out) throws IOException { out.writeDouble(first); out.writeDouble(last); }
        public void readFields(DataInput in) throws IOException { first=in.readDouble(); last=in.readDouble(); }
    }
    /** Sort tiles contiguously while retaining one reduce group per subsystem. */
    public static final class ReconstructionKey implements WritableComparable<ReconstructionKey> {
        public long tile,line;
        public int part;
        public ReconstructionKey() {}
        public ReconstructionKey(long tile,PartitionKey key) { this.tile=tile; line=key.line; part=key.part; }
        public void write(DataOutput out) throws IOException { out.writeLong(tile); out.writeLong(line); out.writeInt(part); }
        public void readFields(DataInput in) throws IOException { tile=in.readLong(); line=in.readLong(); part=in.readInt(); }
        public int compareTo(ReconstructionKey other) {
            int c=Long.compare(tile,other.tile); if(c!=0) return c;
            c=Long.compare(line,other.line); return c!=0?c:Integer.compare(part,other.part);
        }
        public int hashCode() { return Long.hashCode(tile)*31+Long.hashCode(line)*17+part; }
        public boolean equals(Object o) { return o instanceof ReconstructionKey&&compareTo((ReconstructionKey)o)==0; }
        public String toString() { return tile+":"+line+":"+part; }
    }
    /** Unchanged modified coefficients plus the read-only boundary solution. */
    public static final class Update implements Writable {
        public State state=new State();
        public Ends ends=new Ends();
        public Update() {}
        public Update(State state,Ends ends) { this.state=state; this.ends=ends; }
        public void write(DataOutput out) throws IOException { state.write(out); ends.write(out); }
        public void readFields(DataInput in) throws IOException { state.readFields(in); ends.readFields(in); }
    }
    public static final class Chunk implements Writable {
        public int offset,stride,n;
        public double[] values=new double[0];
        public Chunk() {}
        public Chunk(int offset,int stride,double[] values) { this.offset=offset; this.stride=stride; n=values.length; this.values=values; }
        public void write(DataOutput out) throws IOException { out.writeInt(offset); out.writeInt(stride); out.writeInt(n); for(double x:values) out.writeDouble(x); }
        public void readFields(DataInput in) throws IOException {
            offset=in.readInt(); stride=in.readInt(); n=length(in,256);
            if(values.length!=n) values=new double[n]; for(int i=0;i<n;i++) values[i]=in.readDouble();
        }
    }
    public static final class Tile implements Writable {
        public double[] values=new double[0];
        public Tile() {}
        public Tile(double[] values) { this.values=values; }
        public void write(DataOutput out) throws IOException { out.writeInt(values.length); for(double x:values) out.writeDouble(x); }
        public void readFields(DataInput in) throws IOException {
            int n=length(in,256*256*256); if(values.length!=n) values=new double[n];
            for(int i=0;i<n;i++) values[i]=in.readDouble();
        }
    }
}
