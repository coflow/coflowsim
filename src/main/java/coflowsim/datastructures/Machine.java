package coflowsim.datastructures;

/**
 * Information of each machine.
 */
public class Machine {
  int machineID;

  /**
   * Constructor for Machine.
   * 
   * @param machineID
   *          ID of this machine.
   */
  public Machine(int machineID) {
    this.machineID = machineID;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "Machine-" + machineID;
  }
}
