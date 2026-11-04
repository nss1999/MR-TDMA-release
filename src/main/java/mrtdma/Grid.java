package mrtdma;

import org.apache.hadoop.conf.Configuration;

/** Grid dimensions shared by input preparation and all three ADI directions. */
final class Grid {
    final int nx, ny, nz;

    Grid(Configuration conf) {
        int legacy = conf.getInt("tdma.n", 0);
        nx = conf.getInt("tdma.nx", legacy);
        ny = conf.getInt("tdma.ny", legacy);
        nz = conf.getInt("tdma.nz", legacy);
    }

    Grid(String[] args, int trailingArguments, String usage) {
        int dimensions = args.length - trailingArguments;
        if (dimensions != 1 && dimensions != 3) throw new IllegalArgumentException(usage);
        nx = Integer.parseInt(args[0]);
        ny = dimensions == 1 ? nx : Integer.parseInt(args[1]);
        nz = dimensions == 1 ? nx : Integer.parseInt(args[2]);
    }

    void validate(int block) {
        if (block < 2 || block > 256) throw new IllegalArgumentException("Need 2 <= block <= 256");
        for (int length : dimensions()) {
            if (length < 4 || length <= block || length % block == 1)
                throw new IllegalArgumentException("Each axis needs at least two subsystems of length >= 2");
        }
        Math.multiplyExact(Math.multiplyExact((long) nx, ny), nz);
    }

    void configure(Configuration conf, int block) {
        conf.setInt("tdma.nx", nx);
        conf.setInt("tdma.ny", ny);
        conf.setInt("tdma.nz", nz);
        conf.setInt("tdma.block", block);
    }

    int[] dimensions() { return new int[]{nx, ny, nz}; }
    long points() { return (long) nx * ny * nz; }
    String label() { return nx + "x" + ny + "x" + nz; }
    boolean cube() { return nx == ny && ny == nz; }
}
