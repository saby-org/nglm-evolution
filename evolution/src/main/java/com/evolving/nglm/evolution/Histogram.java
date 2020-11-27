package com.evolving.nglm.evolution;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

public class Histogram
{

  String name;
  String units;
  int scale;
  int numberOfBuckets;
  int bucketSize;

  LongAdder overflowBucket;
  LongAdder total;
  LongAccumulator max;
  LongAccumulator min;
  LongAdder eventCount;
  LongAdder[] histogram;

  public Histogram(String name, int bucketSize, int numberOfBuckets, int scale, String units)
  {
    this.name = name;
    this.bucketSize = bucketSize;
    this.numberOfBuckets = numberOfBuckets;
    this.scale = scale;
    this.units = units;
    initializeBuckets();
  }

  void initializeBuckets()
  {
    histogram = new LongAdder[numberOfBuckets];
    for (int i=0; i<numberOfBuckets; i++) histogram[i] = new LongAdder();
    overflowBucket = new LongAdder();
    total = new LongAdder();
    max = new LongAccumulator(Math::max,0);
    min = new LongAccumulator(Math::min,Long.MAX_VALUE);
    eventCount = new LongAdder();
  }

  public void logData(long unscaledObservation)
  {

    long observation = unscaledObservation / scale + ((unscaledObservation % scale != 0L) ? 1L : 0L);

    int bucket = (int) observation / bucketSize;
    if (bucket < numberOfBuckets)
      {
        histogram[bucket].increment();
      }
    else
      {
        overflowBucket.increment();
      }

    eventCount.increment();
    total.add(observation);

    max.accumulate(observation);
    min.accumulate(observation);

  }

  public double getMean()
  {
    return (eventCount.sum() > 0) ? (double) total.sum() / eventCount.sum() : 0.0;
  }

  public long getMinimum()
  {
    return min.get();
  }

  public long getMaximum()
  {
    return max.get();
  }

  public double getPercentile(double p)
  {
    if (eventCount.sum() == 0) return 0.0;

    int bucket = 0;
    int cumulative = 0;
    for (int i=0; i<numberOfBuckets; i++)
      {
        cumulative += histogram[i].sum();
        bucket = i;
        if (((double) cumulative / eventCount.sum()) >= p)
          {
            break;
          }
      }

    double midpoint = (((2 * bucket + 1) * bucketSize) - 1) * 0.5;

    return midpoint;
  }

  public String toString()
  {

    JSONArray buckets = new JSONArray();
    for (int i=0; i<numberOfBuckets; i++)
      {
        JSONObject bucket = new JSONObject();
        bucket.put("bucket", Integer.toString(i*bucketSize));
        bucket.put("count", Long.toString(histogram[i].sum()));
        buckets.add(bucket);
      }

    JSONObject result = new JSONObject();
    DecimalFormat decimalFormat = new DecimalFormat("0.00");
    result.put("histogram", name);
    result.put("units", units);
    result.put("min", (getMinimum() != Long.MAX_VALUE) ? Long.toString(getMinimum()) : "n/a");
    result.put("max", (getMaximum() != -1) ? Long.toString(getMaximum()) : "n/a");
    result.put("avg", decimalFormat.format(getMean()));
    result.put("95%", decimalFormat.format(getPercentile(0.95)));
    result.put("buckets", buckets);
    result.put("over", Long.toString(overflowBucket.sum()));

    return result.toString();
  }
}
