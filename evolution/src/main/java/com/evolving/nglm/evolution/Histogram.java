/****************************************************************************
*
*  Histogram.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.DecimalFormat;

public class Histogram
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  String name;
  String units;
  int scale;
  int numberOfBuckets;
  int bucketSize;
  int overflowBucket;
  long total;
  long max;
  long min;
  int eventCount;
  int[] histogram;

  //
  //  accessors
  //

  int getEventCount() { return eventCount; }

  /*****************************************
  *
  * constructor
  *
  *****************************************/

  public Histogram(String name, int bucketSize, int numberOfBuckets, int scale, String units)
  {
    this.name = name;
    this.bucketSize = bucketSize;
    this.numberOfBuckets = numberOfBuckets;
    this.scale = scale;
    this.units = units;
    initializeBuckets();
  }

  /*****************************************
  *
  * initializeBuckets
  *
  *****************************************/

  void initializeBuckets()
  {
    histogram = new int[numberOfBuckets];
    for (int i=0; i<numberOfBuckets; i++) histogram[i] = 0;
    overflowBucket = 0;
    total = 0;
    max = -1;
    min = Integer.MAX_VALUE;
    eventCount = 0;
  }

  /*****************************************
  *
  * logData
  *
  *****************************************/

  public synchronized void logData(long unscaledObservation)
  {
    //
    //  scale
    //

    long observation = unscaledObservation / scale + ((unscaledObservation % scale != 0L) ? 1L : 0L);

    //
    //  choose bucket
    //

    int bucket = (int) observation / bucketSize;
    if (bucket < numberOfBuckets)
      {
        histogram[bucket]++;
      }
    else
      {
        overflowBucket++;
      }

    //
    //  eventCount and total
    //

    eventCount++;
    total += observation;

    //
    //  min and max
    //

    if (observation > max) max = observation;
    if (observation < min) min = observation;
  }

  /*****************************************
  *
  * reset
  *
  *****************************************/

  public synchronized void reset()
  {
    initializeBuckets();
  }

  /*****************************************
  *
  * getMean
  *
  *****************************************/

  public synchronized double getMean()
  {
    return (eventCount > 0) ? (double) total / eventCount : 0.0;
  }

  /*****************************************
  *
  * getMinimum
  *
  *****************************************/

  public synchronized long getMinimum()
  {
    return min;
  }

  /*****************************************
  *
  * getMaximum
  *
  *****************************************/

  public synchronized long getMaximum()
  {
    return max;
  }

  /*****************************************
  *
  * getPercentile
  *
  *****************************************/

  public synchronized double getPercentile(double p)
  {
    if (eventCount == 0) return 0.0;

    //
    //  get percentile
    //

    int bucket = 0;
    int cumulative = 0;
    for (int i=0; i<numberOfBuckets; i++)
      {
        cumulative += histogram[i];
        bucket = i;
        if (((double) cumulative / eventCount) >= p) 
          {
            break;
          }
      }

    //
    // find the midpoint between the start and end of the bucket
    //

    double midpoint = (((2 * bucket + 1) * bucketSize) - 1) * 0.5;

    //
    //  return
    //

    return midpoint;
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/

  public synchronized String toString()
  {
    //
    //  buckets
    //

    JSONArray buckets = new JSONArray();
    for (int i=0; i<numberOfBuckets; i++)
      {
        JSONObject bucket = new JSONObject();
        bucket.put("bucket", Integer.toString(i*bucketSize));
        bucket.put("count", Integer.toString(histogram[i]));
        buckets.add(bucket);
      }

    //
    //  histogram
    //

    JSONObject result = new JSONObject();
    DecimalFormat decimalFormat = new DecimalFormat("0.00");
    DecimalFormat bucketFormat = new DecimalFormat("###0");
    result.put("histogram", name);
    result.put("units", units);
    result.put("min", (getMinimum() != Integer.MAX_VALUE) ? Long.toString(getMinimum()) : "n/a");
    result.put("max", (getMaximum() != -1) ? Long.toString(getMaximum()) : "n/a");
    result.put("avg", decimalFormat.format(getMean()));
    result.put("95%", decimalFormat.format(getPercentile(0.95)));
    result.put("buckets", buckets);
    result.put("over", Integer.toString(overflowBucket));

    //
    //  result
    //

    return result.toString();
  }
}
