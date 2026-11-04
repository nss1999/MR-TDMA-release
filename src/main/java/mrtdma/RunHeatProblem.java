package mrtdma;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Generate, organize, and solve the built-in heat problem in one invocation. */
public final class RunHeatProblem extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Grid grid = new Grid(args, 7,
                "N or Nx Ny Nz, then block writers width height reducers maps new-run-directory");
        int offset = args.length - 7;
        int block = Integer.parseInt(args[offset]);
        int writers = Integer.parseInt(args[offset + 1]);
        int width = Integer.parseInt(args[offset + 2]);
        int height = Integer.parseInt(args[offset + 3]);
        int reducers = Integer.parseInt(args[offset + 4]);
        int maps = Integer.parseInt(args[offset + 5]);
        grid.validate(block);
        if (writers < 1 || writers > 4) {
            throw new IllegalArgumentException("Need 1..4 writers");
        }
        if (width < 1 || height < 1 || width > 187 || height > 187) {
            throw new IllegalArgumentException("Region dimensions must be in 1..187");
        }
        if (maps < 1 || maps != reducers || !getConf().getBoolean("tdma.spatial.fixed.maps", true)) {
            throw new IllegalArgumentException("Spatial input requires equal positive Map/Reduce counts in fixed-count mode");
        }

        Path root = new Path(args[offset + 6]);
        JobConf conf = new JobConf(getConf());
        FileSystem fs = root.getFileSystem(conf);
        if (fs.exists(root)) {
            throw new IOException("Run directory already exists: " + root);
        }
        // Reserve the run directory before any stage can write to it.
        try (var marker = fs.create(new Path(root, "_RUN_STARTED"), false)) {
            marker.writeUTF(grid.label());
        }

        Path original = new Path(root, "initial-coefficients");
        Path spatial = new Path(root, "spatial-input");
        Path solution = new Path(root, "solution");
        String nx = Integer.toString(grid.nx);
        String ny = Integer.toString(grid.ny);
        String nz = Integer.toString(grid.nz);
        String blockLength = Integer.toString(block);
        String regionWidth = Integer.toString(width);
        String regionHeight = Integer.toString(height);

        PrepareInput.prepare(conf, grid, block, writers, original);
        SpatialRepack repack = new SpatialRepack();
        repack.setConf(conf);
        int status = repack.run(new String[] {
                nx, ny, nz, blockLength, regionWidth, regionHeight, original.toString(), spatial.toString()
        });
        if (status != 0) {
            return status;
        }

        conf.setInt("tdma.slice.width", width);
        conf.setInt("tdma.slice.height", height);
        Driver solver = new Driver();
        solver.setConf(conf);
        return solver.run(new String[] {
                nx, ny, nz, blockLength, Integer.toString(reducers), Integer.toString(maps),
                solution.toString(), spatial.toString()
        });
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new JobConf(), new RunHeatProblem(), args));
    }
}
