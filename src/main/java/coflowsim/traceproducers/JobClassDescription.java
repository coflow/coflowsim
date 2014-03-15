package coflowsim.traceproducers;

/**
 * Used to convey the descriptions of each job classes to
 * {@link coflowsim.traceproducers.CustomTraceProducer}.
 */
public class JobClassDescription {
  public final int minWidth;
  public final int maxWidth;
  public final int minLength;
  public final int maxLength;

  /**
   * Constructor for JobClassDescription.
   * 
   * @param minW
   *          Minimum number of tasks.
   * @param maxW
   *          Maximum number of tasks.
   * @param minL
   *          Minimum shuffle size in MB for each reducer.
   * @param maxL
   *          Maximum shuffle size in MB for each reducer.
   */
  public JobClassDescription(int minW, int maxW, int minL, int maxL) {
    minWidth = Math.max(minW, 1);
    maxWidth = Math.max(maxW, minWidth);
    minLength = Math.max(minL, 1);
    maxLength = Math.max(maxL, minLength);
  }
}
