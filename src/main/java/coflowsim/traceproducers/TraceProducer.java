package coflowsim.traceproducers;

import coflowsim.datastructures.JobCollection;

/**
 * Abstract class for different trace producers.
 */
public abstract class TraceProducer {

  /**
   * {@link JobCollection} object that holds all jobs generated or read by a TraceProducer.
   */
  public final JobCollection jobs;

  /**
   * Constructor for TraceProducer.
   */
  public TraceProducer() {
    jobs = new JobCollection();
  }

  /**
   * Either generates the trace or reads from somewhere.
   */
  public abstract void prepareTrace();

  /**
   * Return the number of racks in the trace.
   * 
   * @return the number of racks.
   */
  public abstract int getNumRacks();

  /**
   * Return the number of machines in each rack of the trace.
   * 
   * @return the number of machines in each rack.
   */
  public abstract int getMachinesPerRack();
}
