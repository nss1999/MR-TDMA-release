package mrtdma;

/** Manufactured solutions test the partitioned solver. */
public final class NumericsCheck {
    public static void main(String[] args) {
        double worst=0;
        int cases=0;
        for(int n:new int[]{16,32,128,1024}) for(int block:new int[]{2,4,8,16,64}) {
            if(block>=n||n%block!=0) continue;
            for(boolean periodic:new boolean[]{false,true}) {
                int parts=n/block;
                double[] expected=new double[n],a=new double[n],b=new double[n],c=new double[n],d=new double[n];
                for(int i=0;i<n;i++) expected[i]=Math.sin(0.013*i)+0.1*Math.cos(0.037*i);
                for(int i=0;i<n;i++) {
                    a[i]=-0.3-0.0001*i; b[i]=4.0+0.0002*i; c[i]=-0.5+0.00001*i;
                    if(!periodic&&i==0) a[i]=0;
                    if(!periodic&&i==n-1) c[i]=0;
                    d[i]=a[i]*expected[(i+n-1)%n]+b[i]*expected[i]+c[i]*expected[(i+1)%n];
                }
                double[][] aa=new double[parts][block],cc=new double[parts][block],dd=new double[parts][block];
                double[] ar=new double[2*parts],br=new double[2*parts],cr=new double[2*parts],dr=new double[2*parts];
                for(int p=0;p<parts;p++) {
                    double[] bb=new double[block];
                    for(int i=0;i<block;i++) { int at=p*block+i; aa[p][i]=a[at]; bb[i]=b[at]; cc[p][i]=c[at]; dd[p][i]=d[at]; }
                    Numerics.reduce(aa[p],bb,cc[p],dd[p]);
                    ar[2*p]=aa[p][0]; ar[2*p+1]=aa[p][block-1];
                    br[2*p]=br[2*p+1]=1.0;
                    cr[2*p]=cc[p][0]; cr[2*p+1]=cc[p][block-1];
                    dr[2*p]=dd[p][0]; dr[2*p+1]=dd[p][block-1];
                }
                if(periodic) Numerics.cyclic(ar,br,cr,dr); else Numerics.thomas(ar,br,cr,dr);
                for(int p=0;p<parts;p++) for(int i=0;i<block;i++) {
                    double actual=i==0?dr[2*p]:i==block-1?dr[2*p+1]:dd[p][i]-aa[p][i]*dr[2*p]-cc[p][i]*dr[2*p+1];
                    double error=Math.abs(actual-expected[p*block+i]); worst=Math.max(worst,error);
                    if(error>1e-12) throw new AssertionError("n="+n+" block="+block+" periodic="+periodic+" at="+(p*block+i)+" error="+error);
                }
                cases++;
            }
        }
        System.out.println("MANUFACTURED_CASES_PASSED="+cases+" MAX_ABSOLUTE_ERROR="+worst);
    }
}
