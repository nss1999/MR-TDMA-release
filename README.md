# MR-TDMA

MR-TDMA solves large tridiagonal systems using Java and Hadoop MapReduce.
The included application applies the solver to one alternating direction implicit
(ADI) time step of a three-dimensional heat problem. Input, intermediate results,
and the final temperature field are stored in HDFS.

## Contents

- [How it works](#how-it-works)
- [Requirements](#requirements)
- [Download and build](#download-and-build)
- [Run on a Hadoop cluster](#run-on-a-hadoop-cluster)
- [Command arguments](#command-arguments)
- [Change the grid or input regions](#change-the-grid-or-input-regions)
- [Output and execution summary](#output-and-execution-summary)
- [Heat-problem setup](#heat-problem-setup)
- [Troubleshooting](#troubleshooting)
- [Source layout](#source-layout)
- [Acknowledgment](#acknowledgment)

## How it works

Each tridiagonal line is divided into local subsystems. For each coordinate
direction, the solver runs the following three Hadoop jobs:

| Stage | Hadoop job | Operation |
| --- | --- | --- |
| Step 1 | Map and Reduce | Read prepared coefficients and reduce each local subsystem with a modified Thomas algorithm. |
| Step 2 | Map only | Gather the subsystem boundary equations for each line and solve the reduced interface system. |
| Step 3 | Map and Reduce | Attach the solved boundary values, reconstruct local solutions in the Reducers, and assemble output tiles. |

The directions are processed in **Z, Y, X** order, giving **nine solution jobs
per ADI step**. After the Z and Y solves, Step 3 also prepares the coefficient
records for the next direction. The X solve produces the final temperature field.
Periodic lines in X and Z use a cyclic tridiagonal solve; Y uses a noncyclic solve.

The workflow below uses a fixed Map count. Spatial input regions organize the
coefficient files; their number does not set the number of Map tasks.

## Requirements

- Linux with Bash and standard command-line utilities, including `sha256sum`.
- Hadoop **3.3.6**, with HDFS and YARN configured for the intended cluster.
- A JDK supporting Java 11 compilation. The build script uses `javac --release 11`.
- `JAVA_HOME` pointing to the JDK, and `hadoop` and `hdfs` available on `PATH`.
- Permission to submit YARN jobs and read/write the HDFS paths used below.

Run the commands from a cluster submission machine whose Hadoop configuration
points to the correct HDFS and YARN services. Worker-node allocation, container
memory, and concurrent task capacity are configured in the cluster separately
from the application arguments.

## Download and build

```bash
git clone https://github.com/nss1999/MR-TDMA-release.git MR-TDMA
cd MR-TDMA
bash scripts/build-java.sh
```

The script compiles the Java sources against `hadoop classpath --glob`, creates
**`build/MR-TDMA.jar`**, and runs the included numerical self-check. Maven is not
required for this build. All three commands used in the workflow below are
included in the same JAR.

The self-check solves 34 manufactured tridiagonal test cases, covering cyclic
and noncyclic systems with several line and subsystem lengths. It fails if an
absolute solution error exceeds `1e-12`. A successful check prints
`MANUFACTURED_CASES_PASSED=34` followed by the maximum absolute error. This checks
the numerical routines; it does not submit cluster jobs.

The build also writes a JAR checksum to `build/MR-TDMA.jar.sha256` and retains
the compiler source list, source checksums, and self-check output under
`build/java-build.*`.

## Run on a Hadoop cluster

Run the following commands from the project directory. The example uses:

| Setting | Value |
| --- | --- |
| Grid | 1024 × 1024 × 1024 |
| Subsystem length along each tridiagonal line | 64 |
| Transverse input-region dimensions | 187 × 187 |
| Map tasks per solution job | 120 |
| Reducers in Steps 1 and 3 | 120 |
| Reducers in Step 2 | 0 |

The paths below are examples. Replace `/user/ubuntu/mrtdma` with an HDFS location
you can write to. **Use a new destination for each preparation command and a new
output root for each solver run.**

### 1. Prepare the initial coefficient records

```bash
hadoop jar build/MR-TDMA.jar mrtdma.PrepareInput \
  1024 64 4 \
  /user/ubuntu/mrtdma/input/n1024/original
```

This generates the initial tridiagonal coefficient records for the built-in heat
problem and writes compressed Hadoop SequenceFiles to HDFS. The `4` selects four
writer threads in this preparation process. It is separate from the solver's
Map count and the cluster's worker count. This command does not submit a
MapReduce job.

### 2. Organize the records into spatial input regions

```bash
hadoop jar build/MR-TDMA.jar mrtdma.SpatialRepack \
  1024 64 187 187 \
  /user/ubuntu/mrtdma/input/n1024/original \
  /user/ubuntu/mrtdma/input/n1024/regions187x187
```

This submits one preprocessing MapReduce job that routes the records into
rectangular regions across the two coordinates transverse to the line direction.
It copies the coefficient values and checks record counts and aggregate record
digests before and after routing. It does not solve the tridiagonal systems.

For this grid, 187 × 187 regions give `ceil(1024/187)^2 = 36` spatial regions.
The solver groups or splits their files to obtain the requested Map count.

### 3. Run the solver

```bash
hadoop jar build/MR-TDMA.jar mrtdma.PartitionedTDMA \
  -Dtdma.slice.width=187 -Dtdma.slice.height=187 \
  1024 64 120 120 \
  /user/ubuntu/mrtdma/output/n1024-run1 \
  /user/ubuntu/mrtdma/input/n1024/regions187x187
```

This submits the nine solution jobs. The two `120` arguments specify the Reducer
count and Map count, respectively. They **do not mean 120 worker nodes**. YARN
schedules the tasks on the workers available to the application.

Initial preparation and spatial repacking are separate from these nine solution
jobs. Reuse the prepared input for another run with the same grid, subsystem
length, and region dimensions, but choose a new solver output root, such as
`/user/ubuntu/mrtdma/output/n1024-run2`.

### Check the requested configuration without running jobs

```bash
hadoop jar build/MR-TDMA.jar mrtdma.PartitionedTDMA \
  -Dtdma.slice.width=187 -Dtdma.slice.height=187 \
  --plan 1024 64 120 120 \
  /user/ubuntu/mrtdma/output/n1024-run1 \
  /user/ubuntu/mrtdma/input/n1024/regions187x187
```

`--plan` prints a JSON configuration prefixed by `MR_TDMA_PLAN`. It submits no
jobs and does not access HDFS. It checks argument validity, but does not verify
that the input exists or that the cluster has enough resources.

## Command arguments

The argument order differs between the preparation commands and the solver:

```text
mrtdma.PrepareInput     N block writers new-HDFS-directory
mrtdma.SpatialRepack    N block width height original-input new-output
mrtdma.PartitionedTDMA  [-D properties] [--plan] N block reducers maps output input
```

Each command also accepts `Nx Ny Nz` in place of `N`, in X, Y, Z order. Use
the same dimensions and block length in all three commands. The three axis
lengths may differ. The solver reports the grid dimensions in X, Y, Z order
and the number of subsystems per line in solve order (Z, Y, X).

| Argument or property | Meaning |
| --- | --- |
| `N` | Number of unknowns along each axis of a cubic grid. |
| `Nx Ny Nz` | Number of unknowns along the X, Y, and Z axes of a rectangular grid. |
| `block` | Number of consecutive unknowns in each local subsystem along a tridiagonal line. For example, N = 1024 and block = 64 give 16 subsystems per line. |
| `writers` | Preparation writer threads, from 1 to 4. |
| `width`, `height` | Transverse region dimensions used by `SpatialRepack`. |
| `reducers` | Reducer tasks in each Step 1 and Step 3 job. Step 2 has no Reducers. |
| `maps` | Requested Map tasks per solution job. For this spatial workflow, it must equal `reducers`. |
| `output` | New HDFS root for the solver run. |
| `input` | HDFS directory produced by `SpatialRepack`. |
| `tdma.slice.width`, `tdma.slice.height` | Region dimensions supplied to the solver; they must match the prepared input. |

Each grid axis must exceed `block`, and `block` must be between 2 and 256.
When an axis length is not divisible by `block`, the last subsystem must
contain at least two rows. Each region dimension must be between 1 and 187.
Use positive, equal Map and Reducer counts with enough input records to give
every Map work. Place Hadoop `-D` properties before the solver's positional
arguments, as shown above.

## Change the grid or input regions

To run a different grid, use the same new `N` and `block` in all three commands
and choose matching new HDFS input paths. For example, a 512³ grid can use
`N=512` and `block=64`. The preparation process generates the problem data;
no external temperature-data file is required for this example.

To change only the spatial grouping, reuse the original coefficient directory
and run `SpatialRepack` with a new destination. Then supply the same dimensions
to the solver:

| Input grouping | `SpatialRepack` arguments | Solver properties |
| --- | --- | --- |
| 187 × 187 | `187 187` | `-Dtdma.slice.width=187 -Dtdma.slice.height=187` |
| 187 × 94 | `187 94` | `-Dtdma.slice.width=187 -Dtdma.slice.height=94` |
| 94 × 94 | `94 94` | `-Dtdma.slice.width=94 -Dtdma.slice.height=94` |

These dimensions change the organization of the input files. They are separate
from the subsystem length, the Map count, and the number of worker nodes.

## Output and execution summary

For the example above, the main outputs are:

```text
/user/ubuntu/mrtdma/output/n1024-run1/
  run-summary.json
  Z/
    step1/
    step2/
    step3/
  Y/
    step1/
    step2/
    step3/
  X/
    step1/
    step2/
    step3/              final temperature field
```

Inspect the summary and final-output directory with:

```bash
hdfs dfs -cat /user/ubuntu/mrtdma/output/n1024-run1/run-summary.json
hdfs dfs -ls /user/ubuntu/mrtdma/output/n1024-run1/X/step3
```

The final field is stored in Hadoop MapFiles under `X/step3/part-r-*`, with
`LongWritable` tile keys and `mrtdma.Records.Tile` values. These are binary Hadoop
records, not CSV files; `hdfs dfs -cat` is suitable for the JSON summary, not for
reading the numerical field as text.

`run-summary.json` records the grid, subsystem length, task counts, job IDs,
verified counters, final-field path, and elapsed times. During execution, the
driver also prints `STAGE_VERIFIED` records and a final `MR_TDMA_RESULT` record.
The driver checks partition, line, and reconstructed-point coverage, as well as
the requested number of nonempty Maps.
Launched Map attempts are recorded separately as `launchedMapAttempts`, since
task retries can increase this count without changing the number of Maps.

`pipelineElapsedSeconds` covers the nine-job solver pipeline, including boundary
cache preparation between jobs. It excludes the separate `PrepareInput` and
`SpatialRepack` commands. Per-job `elapsedSeconds` and whole-pipeline time have
different scopes.

## Heat-problem setup

The included example solves a three-dimensional, time-dependent heat-conduction
problem in a unit cube. The heat-conduction benchmark is described in Section 4.3
of [Kim et al. (2021), *PaScaL_TDMA: A library of parallel and scalable solvers for
massive tridiagonal systems*](https://doi.org/10.1016/j.cpc.2020.107722).
The implementation follows the PaScaL_TDMA `convection_3D` example code.

**Governing equation**

```math
\frac{\partial \theta}{\partial t}
= C_t \left(
\frac{\partial^2 \theta}{\partial x^2}
+ \frac{\partial^2 \theta}{\partial y^2}
+ \frac{\partial^2 \theta}{\partial z^2}
\right).
```

Here, `C_t` is the thermal diffusivity, named `Ct` in the original example
code and `ct` in the Java implementation.

One solver invocation advances this problem by one ADI time step.

| Setting | Implemented value |
| --- | --- |
| Domain lengths | Lx = Ly = Lz = 1 |
| Time step | Δt = 0.005 |
| Number of time steps per solver invocation | 1 |
| X and Z boundaries | Periodic |
| Y boundaries | Lower temperature +1; upper temperature −1 |
| Parameters | Pr = 5, Ra = 200, alphaG = 1 |
| Diffusion coefficient | Ct = nu / Pr, with nu = 1 / sqrt(Ra / (alphaG × Pr × Ly³ × 2)); Ct ≈ 0.04472136 |
| Grid spacing | dx = 1/Nx, dy = 1/(Ny+1), dz = 1/Nz |
| Arithmetic | Double precision |

The initial field is evaluated at `x=i/Nx`, `y=(j+1)/(Ny+1)`, and `z=k/Nz` for
zero-based indices. The implemented Y boundary stencil uses an upper-side
distance of `dy/2` at the last interior row; the remaining Y-side distances are
`dy`. The exact coefficient and right-hand-side expressions are in
[`Numerics.Physics`](src/main/java/mrtdma/Numerics.java).

These physical parameters are defined in the source, rather than supplied as
command-line options. Each invocation starts from the prepared initial problem
and performs one step. Repeating the command with the same input gives repeated
runs of that step; it does not advance a multi-step simulation.

## Troubleshooting

| Message or symptom | What to check |
| --- | --- |
| `Required executable not found` | Set `JAVA_HOME` to a JDK and check that Hadoop is on `PATH`. |
| Empty Hadoop classpath or missing Hadoop classes | Run the build with the intended Hadoop installation and configuration. |
| `Output exists` or `Output already exists` | Choose a new HDFS destination. The commands preserve existing output directories. |
| `Invalid geometry` | Check the N/block divisibility and supported region dimensions described above. |
| Missing or empty spatial regions | Use the completed `SpatialRepack` output and matching width/height properties. |
| `Map coverage` mismatch | Check for empty input splits and confirm that the configured Map count matches the prepared input. |
| YARN containers remain pending or exceed memory limits | Check worker capacity and the cluster's Map/Reduce container and Java heap settings. |

## Source layout

| File | Purpose |
| --- | --- |
| [`scripts/build-java.sh`](scripts/build-java.sh) | Compile, package, and run numerical self-checks. |
| [`PrepareInput.java`](src/main/java/mrtdma/PrepareInput.java) | Generate the initial coefficient files. |
| [`SpatialRepack.java`](src/main/java/mrtdma/SpatialRepack.java) | Organize coefficient records into spatial regions. |
| [`PartitionedTDMA.java`](src/main/java/mrtdma/PartitionedTDMA.java) and [`Driver.java`](src/main/java/mrtdma/Driver.java) | Solver entry point and nine-job execution pipeline. |
| [`Step1.java`](src/main/java/mrtdma/Step1.java), [`Step2.java`](src/main/java/mrtdma/Step2.java), [`Step3.java`](src/main/java/mrtdma/Step3.java) | Local reduction, reduced-system solve, and reconstruction. |
| [`Numerics.java`](src/main/java/mrtdma/Numerics.java) | Numerical routines and heat-problem definition. |
| [`NumericsCheck.java`](src/main/java/mrtdma/NumericsCheck.java) | Manufactured-solution self-checks. |
| [`Records.java`](src/main/java/mrtdma/Records.java) | Hadoop record and tile formats. |

## Acknowledgment

Third-party attribution is provided in [`NOTICE`](NOTICE). The corresponding
copyright and license notice is retained in
[`LICENSES/PaScaL-LICENSE`](LICENSES/PaScaL-LICENSE).
