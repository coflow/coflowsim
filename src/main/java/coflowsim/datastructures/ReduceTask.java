package coflowsim.datastructures;

import java.util.Random;
import java.util.Vector;

/**
 * Information of each reducer.
 */
public class ReduceTask extends Task {

  public double shuffleBytes;
  public double shuffleBytesLeft;
  public Vector<Flow> flows;

  public final double actualShuffleDuration;

  /**
   * <p>
   * Constructor for ReduceTask.
   * </p>
   * 
   * @param taskName
   *          the name of the task.
   * @param taskID
   *          the unique ID of the task.
   * @param parentJob
   *          parent {@link coflowsim.datastructures.Job} of this task.
   * @param startTime
   *          time when the task started.
   * @param taskDuration
   *          duration of the task.
   * @param shuffleBytes
   *          bytes received by this reducer.
   * @param shuffleDuration
   *          time this reducer spent in receiving the data.
   * @param assignedMachine
   *          the {@link coflowsim.datastructures.Machine} where this task ran.
   */
  public ReduceTask(
      String taskName,
      int taskID,
      Job parentJob,
      double startTime,
      double taskDuration,
      Machine assignedMachine,
      double shuffleBytes,
      double shuffleDuration) {

    super(TaskType.REDUCER, taskName, taskID, parentJob, startTime, taskDuration, assignedMachine);

    this.actualShuffleDuration = shuffleDuration;
    this.shuffleBytes = shuffleBytes;
    this.shuffleBytesLeft = shuffleBytes;

    // Rounding to the nearest 1MB
    this.roundToNearestNMB(1);
  }

  /**
   * Create flows given a size without any perturbation.
   */
  public void createFlows() {
    createFlows(false, 0);
  }

  /**
   * Create flows from a given size with perturbation
   * 
   * @param perturb
   *          yes/no
   * @param perc
   *          +/- percentage of perturbation (10 = 10%)
   */
  public void createFlows(boolean perturb, int perc) {
    Random ranGen = new Random(parentJob.numReducers);
    flows = new Vector<Flow>();

    double avgFlowSize = shuffleBytes / parentJob.numMappers;
    for (Task m : parentJob.tasks) {
      if (m.taskType != TaskType.MAPPER) {
        continue;
      }
      MapTask mt = (MapTask) m;

      // Only perturb if asked for and there are more than one flows
      double perturbAmount = 0.0;
      if (perturb && parentJob.numMappers > 1) {
        double mult = 1;
        if (ranGen.nextInt() % 2 == 1) {
          mult = -1;
        }
        perturbAmount = mult * ranGen.nextInt(perc) / 100.0;
      }
      double flowSize = Math.max(avgFlowSize + perturbAmount, 1048576);
      flows.add(new Flow(mt, this, flowSize));
    }
  }

  /**
   * Round {@link #shuffleBytes} to the nearest Megabyte.
   * 
   * @param MB
   *          Megabyte {@link #shuffleBytes} to round up to.
   */
  public void roundToNearestNMB(long MB) {
    long tmp = (long) shuffleBytes;
    long MULT = MB * 1048576;
    long numMB = tmp / MULT;
    if (tmp % MULT > 0) {
      numMB++;
    }
    this.shuffleBytes = MULT * numMB;
    shuffleBytesLeft = this.shuffleBytes;
  }

  /** {@inheritDoc} */
  @Override
  public void resetTaskStates() {
    super.resetTaskStates();
    shuffleBytesLeft = shuffleBytes;
  }

  /** {@inheritDoc} */
  @Override
  public void cleanupTask(long curTime) {
    super.cleanupTask(curTime);
    shuffleBytesLeft = 0.0;
  }
}
