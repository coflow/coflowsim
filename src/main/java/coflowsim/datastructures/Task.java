package coflowsim.datastructures;

import coflowsim.utils.Constants;

/**
 * Abstract class for any {@link coflowsim.datastructures.MapTask} or
 * {@link coflowsim.datastructures.ReduceTask}.
 */
public abstract class Task implements Comparable<Task> {

  public static enum TaskType {
    MAPPER,
    REDUCER
  }

  public final String taskName;

  // FIXME: taskID is used as its rack/machine ID throughout the simulator
  public final int taskID;

  public final TaskType taskType;

  public final Job parentJob;

  public final double actualStartTime;

  public double simulatedStartTime;
  public double simulatedFinishTime;

  public final double taskDuration;

  public final Machine assignedMachine;

  /**
   * <p>
   * Constructor for Task.
   * </p>
   * 
   * @param taskType
   *          {@link coflowsim.datastructures.Task.TaskType} of the task.
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
   * @param assignedMachine
   *          the {@link coflowsim.datastructures.Machine} where this task ran.
   */
  public Task(
      TaskType taskType,
      String taskName,
      int taskID,
      Job parentJob,
      double startTime,
      double taskDuration,
      Machine assignedMachine) {

    this.taskType = taskType;
    this.taskName = taskName;
    this.taskID = taskID;
    this.parentJob = parentJob;
    this.actualStartTime = startTime;
    this.taskDuration = taskDuration;
    this.assignedMachine = assignedMachine;

    resetTaskStates();
  }

  /**
   * Stuff to do when a task starts.
   * 
   * @param curTime
   *          a long.
   */
  public void startTask(long curTime) {
    simulatedStartTime = curTime;
    parentJob.onTaskSchedule(this);
  }

  /**
   * Stuff to do when a task completes.
   * 
   * @param curTime
   *          a long.
   */
  public void cleanupTask(long curTime) {
    simulatedFinishTime = curTime;
    parentJob.onTaskFinish(this);
  }

  /**
   * <p>
   * getPlacement.
   * </p>
   * 
   * @return a int.
   */
  public int getPlacement() {
    return assignedMachine.machineID;
  }

  /**
   * <p>
   * hasStarted
   * </p>
   * 
   * @return a boolean
   */
  public boolean hasStarted() {
    return simulatedStartTime != -1;
  }

  /**
   * <p>
   * isCompleted.
   * </p>
   * 
   * @return a boolean.
   */
  public boolean isCompleted() {
    return simulatedFinishTime != Constants.VALUE_UNKNOWN;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return taskType + "-" + taskName;
  }

  /**
   * <p>
   * compareTo.
   * </p>
   * 
   * @param arg0
   *          a {@link coflowsim.datastructures.Task} object.
   * @return a int.
   */
  public int compareTo(Task arg0) {
    return taskName.compareTo(arg0.taskName);
  }

  /**
   * <p>
   * resetTaskStates.
   * </p>
   */
  public void resetTaskStates() {
    simulatedStartTime = Constants.VALUE_UNKNOWN;
    simulatedFinishTime = Constants.VALUE_UNKNOWN;
  }
}
