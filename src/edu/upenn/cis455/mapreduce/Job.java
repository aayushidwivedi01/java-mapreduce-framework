package edu.upenn.cis455.mapreduce;

// TODO: Auto-generated Javadoc
/**
 * The Interface Job.
 */
public interface Job {

  /**
   * Map.
   *
   * @param key the key
   * @param value the value
   * @param context the context
   */
  void map(String key, String value, Context context);
  
  /**
   * Reduce.
   *
   * @param key the key
   * @param values the values
   * @param context the context
   */
  void reduce(String key, String values[], Context context);
}
