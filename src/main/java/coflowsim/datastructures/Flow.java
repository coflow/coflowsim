package coflowsim.datastructures;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Information about individual flow.
 */
public class Flow implements Comparable<Flow> {
  static AtomicInteger nextId = new AtomicInteger();
  private int id;

  public final MapTask mapper;
  public final ReduceTask reducer;
  private final double totalBytes;

  public double bytesRemaining;
  public double currentBps;
  public boolean consideredAlready;

  /**
   * Constructor for Flow.
   * 
   * @param mapper
   *          flow source.
   * @param reducer
   *          flow destination.
   * @param totalBytes
   *          size in bytes.
   */
  public Flow(MapTask mapper, ReduceTask reducer, double totalBytes) {
    this.id = nextId.incrementAndGet();

    this.mapper = mapper;
    this.reducer = reducer;
    this.totalBytes = totalBytes;

    this.bytesRemaining = totalBytes;
    this.currentBps = 0.0;
    this.consideredAlready = false;
  }

  /**
   * For the Comparable interface.
   */
  public int compareTo(Flow arg0) {
    return id - arg0.id;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "FLOW-" + mapper + "-->" + reducer + " | " + bytesRemaining;
  }

  /**
   * Getter for totalBytes
   * 
   * @return flow size in bytes.
   */
  public double getFlowSize() {
    return totalBytes;
  }
}
