package coflowsim.simulators;

import java.util.Arrays;
import java.util.Vector;

import coflowsim.datastructures.Flow;
import coflowsim.datastructures.Job;
import coflowsim.datastructures.ReduceTask;
import coflowsim.datastructures.Task;
import coflowsim.datastructures.Task.TaskType;
import coflowsim.traceproducers.TraceProducer;
import coflowsim.utils.Constants;
import coflowsim.utils.Constants.SHARING_ALGO;

/**
 * Implements {@link coflowsim.simulators.Simulator} for non-clairvoyant coflow-level scheduling.
 */
public class CoflowSimulatorDark extends CoflowSimulator {

  public static int NUM_JOB_QUEUES = 10;
  public static double INIT_QUEUE_LIMIT = 1048576.0 * 10;
  public static double JOB_SIZE_MULT = 10.0;

  Vector<Job>[] sortedJobs;

  /**
   * {@inheritDoc}
   */
  public CoflowSimulatorDark(SHARING_ALGO sharingAlgo, TraceProducer traceProducer) {

    super(sharingAlgo, traceProducer, false, false, 0.0);
    assert (sharingAlgo == SHARING_ALGO.DARK);
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  protected void initialize(TraceProducer traceProducer) {
    super.initialize(traceProducer);

    this.sortedJobs = (Vector<Job>[]) new Vector[NUM_JOB_QUEUES];
    for (int i = 0; i < NUM_JOB_QUEUES; i++) {
      sortedJobs[i] = new Vector<Job>();
    }
  }

  /**
   * <p>
   * FIFO within each queue Strict priority across queues
   * </p>
   * 
   * @param curTime
   *          current time
   */
  private void updateRates(long curTime) {
    // Reset sendBpsFree and recvBpsFree
    resetSendRecvBpsFree();

    // Recalculate rates
    for (int q = 0; q < NUM_JOB_QUEUES; q++) {
      for (Job sj : sortedJobs[q]) {

        // Calculate the number of mappers and reducers in each port
        int[] numMapSideFlows = new int[NUM_RACKS];
        Arrays.fill(numMapSideFlows, 0);
        int[] numReduceSideFlows = new int[NUM_RACKS];
        Arrays.fill(numReduceSideFlows, 0);
        for (Task t : sj.tasks) {
          if (t.taskType != TaskType.REDUCER) continue;

          ReduceTask rt = (ReduceTask) t;
          int dst = rt.taskID;
          if (recvBpsFree[dst] <= Constants.ZERO) {
            // Set rates to 0 explicitly else it may send at the rate it was assigned last
            for (Flow f : rt.flows) {
              f.currentBps = 0;
            }
            continue;
          }

          for (Flow f : rt.flows) {
            int src = f.mapper.taskID;
            if (sendBpsFree[src] <= Constants.ZERO) {
              // Set rates to 0 explicitly else it may send at the rate it was assigned last
              f.currentBps = 0;
              continue;
            }

            numMapSideFlows[src]++;
            numReduceSideFlows[dst]++;
          }
        }

        double[] sendUsed = new double[NUM_RACKS];
        double[] recvUsed = new double[NUM_RACKS];
        Arrays.fill(sendUsed, 0.0);
        Arrays.fill(recvUsed, 0.0);

        for (Task t : sj.tasks) {
          if (t.taskType != TaskType.REDUCER) continue;

          ReduceTask rt = (ReduceTask) t;
          int dst = rt.taskID;
          if (recvBpsFree[dst] <= Constants.ZERO || numReduceSideFlows[dst] == 0) continue;

          for (Flow f : rt.flows) {
            int src = f.mapper.taskID;
            if (sendBpsFree[src] <= Constants.ZERO || numMapSideFlows[src] == 0) continue;

            // Determine rate based only on this job and available bandwidth
            double minFree = Math.min(sendBpsFree[src] / numMapSideFlows[src],
                recvBpsFree[dst] / numReduceSideFlows[dst]);
            if (minFree <= Constants.ZERO) {
              minFree = 0.0;
            }

            f.currentBps = minFree;

            // Remember how much capacity was allocated
            sendUsed[src] += f.currentBps;
            recvUsed[dst] += f.currentBps;
          }
        }

        // Remove capacity from ALL sources and destination for the entire job
        for (int i = 0; i < NUM_RACKS; i++) {
          sendBpsFree[i] -= sendUsed[i];
          recvBpsFree[i] -= recvUsed[i];
        }
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void afterJobAdmission(long curTime) {
    updateJobOrder();
    layoutFlowsInJobOrder();
    updateRates(curTime);
  }

  /** {@inheritDoc} */
  @Override
  protected void afterJobDeparture(long curTime) {
    updateJobOrder();
    layoutFlowsInJobOrder();
    updateRates(curTime);
  }

  @Override
  protected void addToSortedJobs(Job j) {
    if (sortedJobs[0].contains(j)) {
      return;
    }

    // Add to the end of the first queue
    sortedJobs[0].add(j);
    j.currentJobQueue = 0;
  }

  /**
   * <p>
   * Update job order by FIFO in each queue and move jobs between queues based on total current size
   * </p>
   */
  private void updateJobOrder() {
    for (int i = 0; i < NUM_JOB_QUEUES; i++) {
      Vector<Job> jobsToMove = new Vector<Job>();
      for (Job j : sortedJobs[i]) {
        double size = j.shuffleBytesCompleted;
        int curQ = 0;
        for (double k = INIT_QUEUE_LIMIT; k < size; k *= JOB_SIZE_MULT) {
          curQ += 1;
        }
        if (j.currentJobQueue < curQ) {
          j.currentJobQueue += 1;
          jobsToMove.add(j);
        }
      }
      if (i + 1 < NUM_JOB_QUEUES && jobsToMove.size() > 0) {
        sortedJobs[i].removeAll(jobsToMove);
        sortedJobs[i + 1].addAll(jobsToMove);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void removeDeadJob(Job j) {
    activeJobs.remove(j.jobName);
    for (int i = 0; i < NUM_JOB_QUEUES; i++) {
      if (sortedJobs[i].remove(j)) {
        break;
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void layoutFlowsInJobOrder() {
    for (int i = 0; i < NUM_RACKS; i++) {
      flowsInRacks[i].clear();
    }

    for (int i = 0; i < NUM_JOB_QUEUES; i++) {
      for (Job j : sortedJobs[i]) {
        for (Task r : j.tasks) {
          if (r.taskType != TaskType.REDUCER) {
            continue;
          }

          ReduceTask rt = (ReduceTask) r;
          if (!rt.hasStarted() || rt.isCompleted()) {
            continue;
          }

          flowsInRacks[r.taskID].addAll(rt.flows);
        }
      }
    }
  }
}
