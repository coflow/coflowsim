package coflowsim.datastructures;

/**
 * Information of each mapper.
 * Mostly unused by our {@link coflowsim.simulators.Simulator} except for mapper location.
 */
public class MapTask extends Task {

  /**
   * Constructor for MapTask.
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
   * @param assignedMachine
   *          the {@link coflowsim.datastructures.Machine} where this task ran.
   */
  public MapTask(
      String taskName,
      int taskID,
      Job parentJob,
      double startTime,
      double taskDuration,
      Machine assignedMachine) {

    super(TaskType.MAPPER, taskName, taskID, parentJob, startTime, taskDuration, assignedMachine);
  }
}
