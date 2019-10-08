/*****************************************************************************
*
*  PointBalance.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.PointFulfillmentRequest.PointOperation;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;


public class PointBalance
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("point_balance");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("expirationDates", SchemaBuilder.array(Timestamp.SCHEMA));
    schemaBuilder.field("points", SchemaBuilder.array(Schema.INT32_SCHEMA));
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PointBalance> serde = new ConnectSerde<PointBalance>(schema, false, PointBalance.class, PointBalance::pack, PointBalance::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PointBalance> serde() { return serde; }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private SortedMap<Date,Integer> balances;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public SortedMap<Date,Integer> getBalances() { return balances; }

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  public PointBalance()
  {
    this.balances = new TreeMap<Date,Integer>();
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private PointBalance(SortedMap<Date,Integer> balances)
  {
    this.balances = balances;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public PointBalance(PointBalance pointBalance)
  {
    this.balances = new TreeMap<Date,Integer>(pointBalance.getBalances()); 
  }

  /*****************************************
  *
  *  getBalance
  *
  *****************************************/

  public int getBalance(Date evaluationDate)
  {
    int result = 0;
    for (Date expirationDate : balances.keySet())
      {
        if (evaluationDate.compareTo(expirationDate) <= 0)
          {
            result += balances.get(expirationDate);
          }
      }
    return result;
  }

  /*****************************************
  *
  *  getFirstExpirationDate
  *
  *****************************************/
  
  public Date getFirstExpirationDate(Date evaluationDate)
  {
    Date result = null;
    for (Date expirationDate : balances.keySet())
      {
        if (evaluationDate.compareTo(expirationDate) <= 0)
          {
            result = expirationDate;
            break;
          }
      }
    return result;
  }

  /*****************************************
  *
  *  update
  *
  *****************************************/

  public boolean update(PointOperation operation, int amount, Point point, Date evaluationDate)
  {
    //
    //  validate
    //

    switch (operation)
      {
        case Credit:
          if (! point.getCreditable()) return false;
          break;

        case Debit:
          if (! point.getDebitable()) return false;
          if (amount > getBalance(evaluationDate)) return false;
          break;
      }

    //
    //  normalize
    //

    Iterator<Date> expirationDates = balances.keySet().iterator();
    while (expirationDates.hasNext())
      {
        Date expirationDate = expirationDates.next();
        if (expirationDate.compareTo(evaluationDate) <= 0)
          {
            expirationDates.remove();
          }
      }

    //
    //  operation
    //

    switch (operation)
      {
        case Credit:
          {
            //
            //  expiration date
            //

            Date expirationDate = EvolutionUtilities.addTime(evaluationDate, point.getValidity().getPeriodQuantity(), point.getValidity().getPeriodType(), Deployment.getBaseTimeZone(), point.getValidity().getRoundUp());

            //
            //  adjust (or create) the bucket
            //

            balances.put(expirationDate, (balances.get(expirationDate) != null) ? balances.get(expirationDate) + amount : amount);

            //
            //  merge (if necessary)
            //

            if (point.getValidity().getValidityExtension())
              {
                Date latestExpirationDate = evaluationDate;
                int balance = 0;
                for (Date bucketExpirationDate : balances.keySet())
                  {
                    latestExpirationDate = bucketExpirationDate.after(latestExpirationDate) ? bucketExpirationDate : latestExpirationDate;
                    balance += balances.get(bucketExpirationDate);
                  }
                balances.clear();
                if (balance > 0)
                  {
                    balances.put(latestExpirationDate, balance);
                  }
              }
          }
          break;

        case Debit:
          {
            int remainingAmount = amount;
            while (remainingAmount > 0)
              {
                //
                //  get earliest bucket
                //

                Date expirationDate = balances.firstKey();
                int bucket = balances.get(expirationDate);

                //
                //  adjust the bucket
                //

                if (bucket > remainingAmount)
                  {
                    bucket -= remainingAmount;
                    remainingAmount = 0;
                  }
                else
                  {
                    remainingAmount -= bucket;
                    bucket = 0;
                  }
                balances.put(expirationDate, bucket);

                //
                //  remove the bucket if empty
                //

                if (bucket == 0)
                  {
                    balances.remove(expirationDate);
                  }
              }
          }
          break;
      }

    //
    //  return
    //

    return true;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PointBalance pointBalance = (PointBalance) value;
    Struct struct = new Struct(schema);
    List<Date> expirationDates = new ArrayList<Date>();
    List<Integer> points = new ArrayList<Integer>();
    for (Date expirationDate : pointBalance.getBalances().keySet())
      {
        expirationDates.add(expirationDate);
        points.add(pointBalance.getBalances().get(expirationDate));
      }
    struct.put("expirationDates", expirationDates);
    struct.put("points", points);
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PointBalance unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    SortedMap<Date,Integer> balances = new TreeMap<Date,Integer>();
    List<Date> expirationDates = (List<Date>) valueStruct.get("expirationDates");
    List<Integer> points = (List<Integer>) valueStruct.get("points");
    for (int i=0; i<expirationDates.size(); i++)
      {
        balances.put(expirationDates.get(i), points.get(i));
      }

    //  
    //  return
    //

    return new PointBalance(balances);
  }
}
