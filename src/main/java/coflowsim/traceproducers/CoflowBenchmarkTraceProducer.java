package coflowsim.traceproducers;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import coflowsim.datastructures.Job;
import coflowsim.datastructures.Machine;
import coflowsim.datastructures.MapTask;
import coflowsim.datastructures.ReduceTask;
import coflowsim.datastructures.Task;
import coflowsim.utils.Constants;

/**
 * Reads a trace from the <a href="https://github.com/coflow/coflow-benchmark">coflow-benchmark</a>
 * project.
 * <p>
 * Expected trace format:
 * <ul>
 * <li>Line 1: &lt;Number of Racks&gt; &lt;Number of Jobs&gt;
 * <li>Line i: &lt;Job ID&gt; &lt;Job Arrival Time &gt; &lt;Number of Mappers&gt; &lt;Location of
 * each Mapper&gt; &lt;Number of Reducers&gt; &lt;Location:ShuffleMB of each Reducer&gt;
 * </ul>
 * 
 * <p>
 * Characteristics of the generated trace:
 * <ul>
 * <li>Each rack has at most one mapper and at most one reducer. Historically, this was because
 * production clusters at Facebook and Microsoft are oversubscribed in core-rack links; essentially,
 * simulating rack-level was enough for them. For full-bisection bandwidth networks, setting to the
 * number of machines should result in desired outcome.
 * <li>All tasks of a phase are known when that phase starts, meaning all mappers start together and
 * all reducers do the same.
 * <li>Mapper arrival times are ignored because they are assumed to be over before reducers start;
 * i.e., shuffle start time is job arrival time.
 * <li>Each reducer's shuffle is equally divided across mappers; i.e., reduce-side skew is intact,
 * while map-side skew is lost. This is because shuffle size is logged only at the reducer end.
 * <li>All times are in milliseconds.
 * </ul>
 */
public class CoflowBenchmarkTraceProducer extends TraceProducer {

  private int NUM_RACKS;
  private final int MACHINES_PER_RACK = 1;

  public int numJobs;

  private String pathToCoflowBenchmarkTraceFile;

  /**
   * @param pathToCoflowBenchmarkTraceFile
   *          Path to the file containing the trace.
   */
  public CoflowBenchmarkTraceProducer(String pathToCoflowBenchmarkTraceFile) {
    this.pathToCoflowBenchmarkTraceFile = pathToCoflowBenchmarkTraceFile;
  }

  /**
   * Read trace from file.
   */
  @Override
  public void prepareTrace() {
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(pathToCoflowBenchmarkTraceFile);
    } catch (FileNotFoundException e) {
      System.err.println("Couldn't open " + pathToCoflowBenchmarkTraceFile);
      System.exit(1);
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(fis));

    // Read number of racks and number of jobs in the trace
    try {
      String line = br.readLine();
      String[] splits = line.split("\\s+");

      NUM_RACKS = Integer.parseInt(splits[0]);
      numJobs = Integer.parseInt(splits[1]);
    } catch (IOException e) {
      System.err.println("Missing trace description in " + pathToCoflowBenchmarkTraceFile);
      System.exit(1);
    }

    // Read numJobs jobs from the trace file
    for (int j = 0; j < numJobs; j++) {
      try {
        String line = br.readLine();
        String[] splits = line.split("\\s+");
        int lIndex = 0;

        String jobName = "JOB-" + splits[lIndex++];
        Job job = jobs.getOrAddJob(jobName);

        int jobArrivalTime = Integer.parseInt(splits[lIndex++]);

        // #region: Create mappers
        int numMappers = Integer.parseInt(splits[lIndex++]);
        for (int mID = 0; mID < numMappers; mID++) {
          String taskName = "MAPPER-" + mID;
          int taskID = mID;

          // 1 <= rackIndex <= NUM_RACKS
          int rackIndex = Integer.parseInt(splits[lIndex++]) + 1;

          // Create map task
          Task task = new MapTask(taskName, taskID, job, jobArrivalTime, Constants.VALUE_IGNORED,
              new Machine(rackIndex));

          // Add task to corresponding job
          job.addTask(task);
        }
        // #endregion

        // #region: Create reducers
        int numReducers = Integer.parseInt(splits[lIndex++]);
        for (int rID = 0; rID < numReducers; rID++) {
          String taskName = "REDUCER-" + rID;
          int taskID = rID;

          // 1 <= rackIndex <= NUM_RACKS
          String rack_MB = splits[lIndex++];

          int rackIndex = Integer.parseInt(rack_MB.split(":")[0]) + 1;
          double shuffleBytes = Double.parseDouble(rack_MB.split(":")[1]) * 1048576.0;

          // Create reduce task
          Task task = new ReduceTask(taskName, taskID, job, jobArrivalTime, Constants.VALUE_IGNORED,
              new Machine(rackIndex), shuffleBytes, Constants.VALUE_IGNORED);

          // Add task to corresponding job
          job.addTask(task);
        }
        // #endregion

      } catch (IOException e) {
        System.err.println("Missing job in " + pathToCoflowBenchmarkTraceFile + ". " + j + "/"
            + numJobs + " read.");
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public int getNumRacks() {
    return NUM_RACKS;
  }

  /** {@inheritDoc} */
  @Override
  public int getMachinesPerRack() {
    return MACHINES_PER_RACK;
  }
}
