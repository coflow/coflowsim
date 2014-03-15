package coflowsim.datastructures;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

/**
 * Keep track of jobs in a {@link coflowsim.traceproducers.TraceProducer}
 */
public class JobCollection implements Iterable<Job> {
  private final HashMap<String, Job> hashOfJobs;
  private final Vector<Job> listOfJobs;

  /**
   * Constructor for JobCollection.
   */
  public JobCollection() {
    hashOfJobs = new HashMap<String, Job>();
    listOfJobs = new Vector<Job>();
  }

  /**
   * Returns the reference to a job given its name or creates a {@link coflowsim.datastructures.Job}
   * object if it doesn't exist.
   * 
   * @param jobName
   *          the name of the job.
   * 
   * @return a reference to the {@link coflowsim.datastructures.Job} with the given name.
   */
  public Job getOrAddJob(String jobName) {
    if (hashOfJobs.containsKey(jobName)) {
      return hashOfJobs.get(jobName);
    } else {
      Job job = new Job(jobName, listOfJobs.size());

      hashOfJobs.put(jobName, job);
      listOfJobs.add(job);

      return job;
    }
  }

  /**
   * Sort all jobs in this collection by their actual start time in the trace.
   */
  public void sortByStartTime() {
    Collections.sort(listOfJobs, new Comparator<Job>() {
      public int compare(Job o1, Job o2) {
        if (o1.actualStartTime == o2.actualStartTime) {
          return 0;
        }
        return o1.actualStartTime < o2.actualStartTime ? -1 : 1;
      }
    });
  }

  /**
   * Returns an iterator of the underlying list containing all the jobs.
   * 
   * @return a {@link java.util.Iterator} over jobs.
   */
  public Iterator<Job> iterator() {
    return listOfJobs.iterator();
  }

  /**
   * Removes the given job from the collection.
   * 
   * @param job
   *          {@link coflowsim.datastructures.Job} to remove.
   */
  public void removeJob(Job job) {
    hashOfJobs.remove(job);
    listOfJobs.remove(job);
  }

  /**
   * Returns the number of jobs in collection.
   * 
   * @return the number of jobs in this collection.
   */
  public int size() {
    return listOfJobs.size();
  }

  /**
   * Returns a specific job at the given index.
   * 
   * @param index
   *          job index in the underlying list.
   * @return a reference to the {@link coflowsim.datastructures.Job} object at the given index.
   */
  public Job elementAt(int index) {
    return listOfJobs.elementAt(index);
  }
}
