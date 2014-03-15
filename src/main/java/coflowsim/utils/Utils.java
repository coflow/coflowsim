package coflowsim.utils;

/**
 * Common utility functions used everywhere.
 */
public class Utils {

  /**
   * Returns the maximum value of a double array.
   * 
   * @param array
   *          an array of double.
   * @return a double.
   */
  public static double max(double[] array) {
    double max = Double.MIN_VALUE;
    for (int i = 0; i < array.length; i++) {
      if (array[i] > max) max = array[i];
    }
    return max;
  }

  /**
   * Returns the sum of a double array.
   * 
   * @param array
   *          an array of double.
   * @return a double.
   */
  public static double sum(double[] array) {
    double sum = 0.0;
    for (int i = 0; i < array.length; i++) {
      sum += array[i];
    }
    return sum;
  }
}
