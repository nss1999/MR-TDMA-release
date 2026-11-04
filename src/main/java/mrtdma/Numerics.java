package mrtdma;

/** Double precision CPU equations from PaScaL_TDMA's convection_3D example. */
public strictfp final class Numerics {
    private Numerics() {}

    public static void thomas(double[] a, double[] b, double[] c, double[] d) {
        int n = d.length;
        d[0] /= b[0]; c[0] /= b[0];
        for (int i = 1; i < n; i++) {
            double r = 1.0 / (b[i] - a[i] * c[i - 1]);
            d[i] = r * (d[i] - a[i] * d[i - 1]);
            c[i] = r * c[i];
        }
        for (int i = n - 2; i >= 0; i--) d[i] -= c[i] * d[i + 1];
    }

    /** Exact CPU tdma_cycl_single recurrence, rather than a new periodic solver. */
    public static void cyclic(double[] a, double[] b, double[] c, double[] d) {
        int n = d.length;
        if (n < 3) throw new IllegalArgumentException("Cyclic system needs at least 3 rows");
        double[] e = new double[n];
        e[1] = -a[1]; e[n - 1] = -c[n - 1];
        d[1] /= b[1]; e[1] /= b[1]; c[1] /= b[1];
        for (int i = 2; i < n; i++) {
            double r = 1.0 / (b[i] - a[i] * c[i - 1]);
            d[i] = r * (d[i] - a[i] * d[i - 1]);
            e[i] = r * (e[i] - a[i] * e[i - 1]); c[i] = r * c[i];
        }
        for (int i = n - 2; i >= 1; i--) {
            d[i] -= c[i] * d[i + 1]; e[i] -= c[i] * e[i + 1];
        }
        d[0] = (d[0] - a[0] * d[n - 1] - c[0] * d[1]) /
               (b[0] + a[0] * e[n - 1] + c[0] * e[1]);
        for (int i = 1; i < n; i++) d[i] += d[0] * e[i];
    }

    /** Modified Thomas reduction in PaScaL_TDMA_single_solve[_cycle]. */
    public static void reduce(double[] a, double[] b, double[] c, double[] d) {
        int n = d.length;
        if (n < 2) throw new IllegalArgumentException("Partition needs at least 2 rows");
        a[0] /= b[0]; d[0] /= b[0]; c[0] /= b[0];
        a[1] /= b[1]; d[1] /= b[1]; c[1] /= b[1];
        // With two rows, both unknowns are already endpoints.
        if(n==2) return;
        for (int i = 2; i < n; i++) {
            double r = 1.0 / (b[i] - a[i] * c[i - 1]);
            d[i] = r * (d[i] - a[i] * d[i - 1]);
            c[i] = r * c[i]; a[i] = -r * a[i] * a[i - 1];
        }
        // Backward elimination starts at zero-based index n-3;
        // the penultimate row already refers directly to the last endpoint.
        for (int i = n - 3; i >= 1; i--) {
            d[i] -= c[i] * d[i + 1];
            a[i] -= c[i] * a[i + 1]; c[i] = -c[i] * c[i + 1];
        }
        double r = 1.0 / (1.0 - a[1] * c[0]);
        d[0] = r * (d[0] - c[0] * d[1]); a[0] = r * a[0];
        c[0] = -r * c[0] * c[1];
    }

    public static final class Physics {
        public final int nx, ny, nz;
        public final double dt = 0.005, dx, dy, dz, ct;
        private final double[] sx, sy, sz;
        public Physics(int n) { this(n,n,n); }
        public Physics(int nx,int ny,int nz) {
            this.nx=nx; this.ny=ny; this.nz=nz;
            dx=1.0/nx; dy=1.0/(ny+1.0); dz=1.0/nz;
            double pr = 5.0, ra = 200.0, alphaG = 1.0, ly = 1.0;
            double nu = 1.0 / Math.sqrt(ra / (alphaG * pr * Math.pow(ly, 3.0) * 2.0));
            ct = nu / pr;
            sx=new double[nx]; sy=new double[ny]; sz=new double[nz];
            for (int i=0;i<nx;i++) sx[i]=Math.sin(4.0*Math.PI*(i*dx));
            for (int i=0;i<ny;i++) sy[i]=Math.sin(4.0*Math.PI*((i+1)*dy));
            for (int i=0;i<nz;i++) sz[i]=Math.sin(4.0*Math.PI*(i*dz));
        }
        public double theta(int x, int y, int z) {
            if (y < 0) return 1.0;
            if (y >= ny) return -1.0;
            x = Math.floorMod(x, nx); z = Math.floorMod(z, nz);
            return -2.0 * ((y + 1) * dy) + 1.0 + sx[x] * sz[z] * sy[y];
        }
        public double lowerStep(int zeroY) { return dy; }
        public double upperStep(int zeroY) { return zeroY == ny - 1 ? dy / 2.0 : dy; }
        public void coefficients(int axis, int row, double[] a, double[] b, double[] c, int at) {
            double h = axis == 0 ? dx : axis == 1 ? dy : dz;
            double lo = h, hi = axis == 1 ? upperStep(row) : h;
            double lowerFlag = axis == 1 && row == 0 ? 0 : 1;
            double upperFlag = axis == 1 && row == ny - 1 ? 0 : 1;
            a[at] = -0.5 * ct / h / lo * lowerFlag * dt;
            c[at] = -0.5 * ct / h / hi * upperFlag * dt;
            b[at] = 0.5 * ct / h * (1.0 / hi + 1.0 / lo) * dt + 1.0;
        }
        public double initialRhs(int x, int y, int z) {
            double t = theta(x,y,z), tim = theta(x-1,y,z), tip = theta(x+1,y,z);
            double tjm = theta(x,y-1,z), tjp = theta(x,y+1,z);
            double tkm = theta(x,y,z-1), tkp = theta(x,y,z+1);
            double jm = lowerStep(y), jp = upperStep(y);
            double jem = y == 0 ? 0 : 1, jep = y == ny-1 ? 0 : 1;
            double dedx1=(t-tim)/dx, dedx2=(tip-t)/dx;
            double dedy3=(t-tjm)/jm, dedy4=(tjp-t)/jp;
            double dedz5=(t-tkm)/dz, dedz6=(tkp-t)/dz;
            double viscous=0.5*ct*((dedx2-dedx1)/dx+(dedy4-dedy3)/dy+(dedz6-dedz5)/dz);
            double ebcDown=0.5*ct/dy/jm, ebcUp=0.5*ct/dy/jp*(-1.0);
            double ebc=(1.0-jem)*ebcDown+(1.0-jep)*ebcUp;
            double eAPI=-0.5*ct/dx/dx, eAMI=-0.5*ct/dx/dx;
            double eACI=0.5*ct/dx*(1.0/dx+1.0/dx);
            double eAPK=-0.5*ct/dz/dz, eAMK=-0.5*ct/dz/dz;
            double eACK=0.5*ct/dz*(1.0/dz+1.0/dz);
            double eAPJ=-0.5*ct/dy*(1.0/jp)*jep, eAMJ=-0.5*ct/dy*(1.0/jm)*jem;
            double eACJ=0.5*ct/dy*(1.0/jp+1.0/jm);
            double eRHS=eAPK*tkp+eACK*t+eAMK*tkm+eAPJ*tjp+eACJ*t+eAMJ*tjm
                       +eAPI*tip+eACI*t+eAMI*tim;
            return (t/dt+viscous+ebc-(t/dt+eRHS))*dt;
        }
    }
}
