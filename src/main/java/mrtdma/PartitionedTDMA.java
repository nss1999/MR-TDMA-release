package mrtdma;

/** Public command-line entry point for the three-stage MR-TDMA solver.
 * The actual job orchestration is in Driver; task logic is in Step1/2/3. */
public final class PartitionedTDMA extends Driver {
    public static void main(String[] args) throws Exception { Driver.main(args); }
}
