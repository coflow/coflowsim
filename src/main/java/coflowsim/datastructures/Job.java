package coflowsim.datastructures;

import java.util.Arrays;
import java.util.Vector;

import coflowsim.datastructures.Task.TaskType;
import coflowsim.utils.Constants;
import coflowsim.utils.Utils;

/**
 * Information about individual Job/Coflow.
 */
public class Job implements Comparable<Job> {

  public final String jobName;
  public final int jobID;

  public double actualStartTime;

  // TODO: All three below are unused; should be removed.
  public double actualFinishTime;
  public double actualShuffleStartTime;
  public double actualShuffleFinishTime;

  public double simulatedStartTime;
  public double simulatedFinishTime;
  public double simulatedShuffleIndividualSums;

  public Vector<Task> tasks;

  public Vector<ReduceTask>[] tasksInRacks;
  public double[] shuffleBytesPerRack;
  public double shuffleBytesCompleted;

  int[] numMappersInRacks;

  public int currentJobQueue;

  // Number of mappers and reducers in the original job
  public int actualNumMappers;
  public int actualNumReducers;

  // Number of mappers and reducers
  public int numMappers;
  public int numReducers;
  public int numActiveReducers;

  public double alpha;
  public double deadlineDuration;
  public boolean wasAdmitted;

  // Shuffle byte stats
  public double totalShuffleBytes;
  public double maxShuffleBytes;

  // Total bytes written
  public double totalHdfsWriteBytes;

  public boolean jobActive;
  public int numActiveTasks;
  private int numStartedTasks;

  /**
   * Constructor for Job.
   * 
   * @param jobName
   *          the name of the job.
   * @param jobID
   *          a unique ID of the job.
   */
  public Job(String jobName, int jobID) {
    this.jobName = jobName;
    this.jobID = jobID;

    actualStartTime = Double.MAX_VALUE;
    actualFinishTime = 0;
    actualShuffleStartTime = 0;
    actualShuffleFinishTime = 0;

    tasks = new Vector<Task>();

    numMappers = 0;
    numReducers = 0;

    maxShuffleBytes = 0;
    totalShuffleBytes = 0;
    totalHdfsWriteBytes = 0;

    resetJobStates();
  }

  /**
   * Adds a task to the job and updates relevant bookkeeping.
   * 
   * @param task
   *          {@link coflowsim.datastructures.Task} to add.
   */
  public void addTask(Task task) {

    tasks.add(task);

    // Determine job arrival and departure times
    if (task.actualStartTime < actualStartTime) {
      actualStartTime = task.actualStartTime;
    }
    if (task.actualStartTime + task.taskDuration > actualFinishTime) {
      actualFinishTime = task.actualStartTime + task.taskDuration;
    }

    // Determine shuffle start and finish times
    if (task.taskType == Task.TaskType.MAPPER) {
      if (task.actualStartTime + task.taskDuration > actualShuffleStartTime) {
        actualShuffleStartTime = task.actualStartTime + task.taskDuration;
      }
    } else if (task.taskType == Task.TaskType.REDUCER) {
      if (task.actualStartTime > actualShuffleStartTime) {
        actualShuffleStartTime = task.actualStartTime;
      }

      ReduceTask rTask = (ReduceTask) task;
      if (task.actualStartTime + rTask.actualShuffleDuration > actualShuffleFinishTime) {
        actualShuffleFinishTime = task.actualStartTime + rTask.actualShuffleDuration;
      }
    }

    // Increase respective task counts
    if (task.taskType == Task.TaskType.MAPPER) {
      numMappers++;
      actualNumMappers++;
    } else if (task.taskType == Task.TaskType.REDUCER) {
      ReduceTask rt = (ReduceTask) task;
      numReducers++;
      actualNumReducers++;
      totalShuffleBytes += rt.shuffleBytes;
      if (rt.shuffleBytes > maxShuffleBytes) {
        maxShuffleBytes = rt.shuffleBytes;
      }
    }
  }

  /**
   * Stuff to do when a task gets scheduled.
   * 
   * @param task
   *          {@link coflowsim.datastructures.Task} under consideration.
   */
  public void onTaskSchedule(Task task) {
    if (task.taskType != TaskType.REDUCER) {
      return;
    }

    if (simulatedStartTime == Constants.VALUE_UNKNOWN
        || simulatedStartTime > task.simulatedStartTime) {
      simulatedStartTime = task.simulatedStartTime;
    }

    numActiveTasks++;
    numStartedTasks++;
    jobActive = true;
  }

  /**
   * Stuff to do when a task completes.
   * 
   * @param task
   *          {@link coflowsim.datastructures.Task} under consideration.
   */
  public void onTaskFinish(Task task) {

    if (task.taskType != TaskType.REDUCER) {
      return;
    }

    if (simulatedFinishTime == Constants.VALUE_UNKNOWN
        || simulatedFinishTime < task.simulatedFinishTime) {
      simulatedFinishTime = task.simulatedFinishTime;
    }
    simulatedShuffleIndividualSums += (task.simulatedFinishTime - task.simulatedStartTime);

    tasksInRacks[task.taskID].remove((ReduceTask) task);
    if (tasksInRacks[task.taskID].size() == 0) {
      tasksInRacks[task.taskID] = null;
    }

    numActiveTasks--;
    numActiveReducers--;
    if (numActiveTasks == 0 && numStartedTasks == numReducers) {
      jobActive = false;
    }
  }

  /**
   * Coflow completion time as determined by the simulator.
   * 
   * @return coflow completion time or {@link coflowsim.utils.Constants#VALUE_UNKNOWN} on error.
   */
  public double getSimulatedDuration() {
    if (simulatedStartTime < 0 || simulatedFinishTime < 0) {
      return Constants.VALUE_UNKNOWN;
    }
    return simulatedFinishTime - simulatedStartTime;
  }

  /**
   * Performs pre-processing for {@link coflowsim.simulators.Simulator#mergeTasksByRack()} using
   * {@link #coalesceMappers(int)}, {@link #coalesceReducers(int)}, and
   * {@link #calcAlphaDeadline(int, double)}.
   * 
   * @param numRacks
   *          number of racks in the simulation
   * @param machinesPerRack
   *          number of machines in each rack
   * @param deadlineMult
   *          deadline duration multiplier
   */
  @SuppressWarnings("unchecked")
  public void arrangeTasks(int numRacks, int machinesPerRack, double deadlineMult) {
    if (numMappersInRacks == null) {
      numMappersInRacks = new int[numRacks];
      Arrays.fill(numMappersInRacks, 0);
    }

    if (tasksInRacks == null) {
      tasksInRacks = (Vector<ReduceTask>[]) new Vector[numRacks];

      shuffleBytesPerRack = new double[numRacks];
      Arrays.fill(shuffleBytesPerRack, 0.0);
    }

    for (Task t : tasks) {
      if (t.taskType == TaskType.MAPPER) {
        MapTask mt = (MapTask) t;

        int fromRack = convertMachineToRack(mt.getPlacement(), machinesPerRack);
        numMappersInRacks[fromRack]++;
      }

      if (t.taskType == TaskType.REDUCER) {
        ReduceTask rt = (ReduceTask) t;

        int toRack = convertMachineToRack(rt.getPlacement(), machinesPerRack);
        if (tasksInRacks[toRack] == null) {
          tasksInRacks[toRack] = new Vector<ReduceTask>();
        }

        addAscending(tasksInRacks[toRack], rt);
        shuffleBytesPerRack[toRack] += rt.shuffleBytes;
      }
    }

    coalesceMappers(numRacks);
    coalesceReducers(numRacks);

    calcAlphaDeadline(numRacks, deadlineMult);
  }

  private void coalesceMappers(int numRacks) {
    Vector<Task> newMappers = new Vector<Task>();
    for (Task t : tasks) {
      if (t.taskType == TaskType.MAPPER) {
        newMappers.add(t);
      }
    }
    tasks.removeAll(newMappers);
    newMappers.clear();

    // Reset mapper counters in job
    numMappers = 0;

    for (int i = 0; i < numRacks; i++) {
      if (numMappersInRacks[i] > 0) {
        numMappers++;

        MapTask iThMt = new MapTask("JOB-" + jobID + "-MAP-" + i, i, this, actualStartTime,
            Constants.VALUE_UNKNOWN, new Machine(i + 1));

        newMappers.add(iThMt);
      }
    }
    tasks.addAll(newMappers);
  }

  private void coalesceReducers(int numRacks) {
    Vector<Task> newReducers = new Vector<Task>();
    for (Task t : tasks) {
      if (t.taskType == TaskType.REDUCER) {
        newReducers.add(t);
      }
    }
    tasks.removeAll(newReducers);
    newReducers.clear();

    // Reset shuffle counters in job
    numReducers = 0;
    totalShuffleBytes = 0.0;
    maxShuffleBytes = 0.0;

    for (int i = 0; i < numRacks; i++) {
      if (tasksInRacks[i] != null && tasksInRacks[i].size() > 0) {
        numReducers++;

        ReduceTask iThRt = new ReduceTask("JOB-" + jobID + "-REDUCE-" + i, i, this, actualStartTime,
            Constants.VALUE_UNKNOWN, new Machine(i + 1), 0, Constants.VALUE_UNKNOWN);

        // Update shuffle counters in task
        for (ReduceTask rt : tasksInRacks[i]) {
          iThRt.shuffleBytes += rt.shuffleBytes;
        }
        iThRt.shuffleBytesLeft = iThRt.shuffleBytes;

        // Update shuffle counters in job
        totalShuffleBytes += iThRt.shuffleBytes;
        maxShuffleBytes = Math.max(maxShuffleBytes, iThRt.shuffleBytes);

        newReducers.add(iThRt);

        tasksInRacks[i].clear();
        tasksInRacks[i].add(iThRt);
      }
    }

    numActiveReducers = numReducers;
    tasks.addAll(newReducers);

    // #region: Fixes for two-sided simulation
    if (numMappers == 0) {
      return;
    }

    totalShuffleBytes = 0;
    maxShuffleBytes = 0;
    for (Task t : tasks) {
      if (t.taskType != TaskType.REDUCER) {
        continue;
      }

      ReduceTask rt = (ReduceTask) t;
      // Rounding to numMappers
      rt.roundToNearestNMB(numMappers);

      totalShuffleBytes += rt.shuffleBytes;
      maxShuffleBytes = Math.max(maxShuffleBytes, rt.shuffleBytes);

      // Now create flows
      rt.createFlows();
      // rt.createFlows(true, 0);
    }

    // #endregion:
  }

  private void addAscending(Vector<ReduceTask> coll, ReduceTask rt) {
    if (coll == null) {
      coll = new Vector<ReduceTask>();
    }

    int index = 0;
    for (; index < coll.size(); index++) {
      if (coll.elementAt(index).shuffleBytesLeft > rt.shuffleBytesLeft) {
        break;
      }
    }
    coll.add(index, rt);
  }

  private int convertMachineToRack(int machine, int machinesPerRack) {
    // Subtracting because machine IDs start from 1
    return (machine - 1) / machinesPerRack;
  }

  /**
   * Determine the remaining size of the coflow.
   * 
   * @return the number of bytes remaining of this coflow.
   */
  public double calcShuffleBytesLeft() {
    return Utils.sum(shuffleBytesPerRack);
  }

  /**
   * Update bytes remaining for a given rack.
   * 
   * @param rackID
   *          index of the rack
   * @param decreaseBy
   *          bytes transferred (i.e., to delete)
   */
  public void decreaseShuffleBytesPerRack(int rackID, double decreaseBy) {
    shuffleBytesPerRack[rackID] -= decreaseBy;
    if (shuffleBytesPerRack[rackID] < 0.0) {
      shuffleBytesPerRack[rackID] = 0.0;
    }
  }

  /**
   * For the Comparable interface.
   */
  public int compareTo(Job arg0) {
    return jobName.compareTo(arg0.jobName);
  }

  private void resetJobStates() {
    simulatedStartTime = Constants.VALUE_UNKNOWN;
    simulatedFinishTime = Constants.VALUE_UNKNOWN;
    simulatedShuffleIndividualSums = 0.0;

    tasksInRacks = null;
    shuffleBytesPerRack = null;
    shuffleBytesCompleted = 0;

    currentJobQueue = 0;

    alpha = Constants.VALUE_UNKNOWN;
    deadlineDuration = Constants.VALUE_UNKNOWN;
    wasAdmitted = false;

    jobActive = false;
    numActiveReducers = 0;
    numActiveTasks = 0;
    numStartedTasks = 0;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return jobName + "(" + numMappers + "/" + numReducers + " | " + shuffleBytesCompleted + "/"
        + totalShuffleBytes + ")";
  }

  /**
   * Calculate and store the initial/static alpha of the coflow (in bytes)
   * 
   * @param numRacks
   *          number of racks in the trace
   * @param deadlineMult
   *          deadline duration multiplier
   */
  private void calcAlphaDeadline(int numRacks, double deadlineMult) {
    // Calculate Alpha
    double[] sendBytes = new double[numRacks];
    Arrays.fill(sendBytes, 0.0);
    double[] recvBytes = new double[numRacks];
    Arrays.fill(recvBytes, 0.0);

    double perMapperBytes = totalShuffleBytes / numMappers;

    for (Task t : tasks) {
      if (t.taskType == TaskType.MAPPER) {
        MapTask mt = (MapTask) t;
        sendBytes[mt.taskID] += perMapperBytes;
      }

      if (t.taskType == TaskType.REDUCER) {
        ReduceTask rt = (ReduceTask) t;
        recvBytes[rt.taskID] += rt.shuffleBytes;
      }
    }

    alpha = Math.max(Utils.max(sendBytes), Utils.max(recvBytes));

    // Calculate deadline in millis
    deadlineDuration = alpha / Constants.RACK_BYTES_PER_SEC * deadlineMult * 1000;
  }

  /**
   * Calculates time remaining from curTime until the deadline.
   * 
   * @param curTime
   *          current simulation time
   * @return time until deadline
   */
  public double timeTillDeadline(long curTime) {
    return simulatedStartTime + deadlineDuration - curTime;
  }
}
