/*****************************************************************************
*
*  Product.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

public class OfferSalesChannelsAndPrice
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
    schemaBuilder.name("salesChannelsAndPrice");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("salesChannelIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("price", OfferPrice.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<OfferSalesChannelsAndPrice> serde = new ConnectSerde<OfferSalesChannelsAndPrice>(schema, false, OfferSalesChannelsAndPrice.class, OfferSalesChannelsAndPrice::pack, OfferSalesChannelsAndPrice::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<OfferSalesChannelsAndPrice> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private List<String> salesChannelIDs;
  private OfferPrice price;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public List<String> getSalesChannelIDs() { return salesChannelIDs ;};
  public OfferPrice getPrice() { return price; };
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public OfferSalesChannelsAndPrice(SchemaAndValue schemaAndValue, List<String> salesChannelIDs, OfferPrice price)
  {
    this.salesChannelIDs = salesChannelIDs;
    this.price = price;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    OfferSalesChannelsAndPrice offerSalesChannelsAndPrice = (OfferSalesChannelsAndPrice) value;
    Struct struct = new Struct(schema);
    struct.put("salesChannelIDs", packSalesChannelIDs(offerSalesChannelsAndPrice.getSalesChannelIDs()));
    if(offerSalesChannelsAndPrice.getPrice() != null){
      struct.put("price", OfferPrice.pack(offerSalesChannelsAndPrice.getPrice()));
    }
    return struct;
  }
  
  /****************************************
  *
  *  packSalesChannelIDs
  *
  ****************************************/

  private static List<Object> packSalesChannelIDs(List<String> salesChannelIDs)
  {
    List<Object> result = new ArrayList<Object>();
    for (String salesChannelsID : salesChannelIDs)
      {
        result.add(salesChannelsID);
      }
    return result;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static OfferSalesChannelsAndPrice unpack(SchemaAndValue schemaAndValue)
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
    List<String> salesChannelIDs = (List<String>) valueStruct.get("salesChannelIDs");
    OfferPrice price = null;
    if(valueStruct.get("price") != null){
      price = OfferPrice.unpack(new SchemaAndValue(schema.field("price").schema(), valueStruct.get("price")));
    }
    
    //
    //  return
    //

    return new OfferSalesChannelsAndPrice(schemaAndValue, salesChannelIDs, price);
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public OfferSalesChannelsAndPrice(JSONObject jsonRoot) throws GUIManagerException
  {
  
    this.salesChannelIDs = decodeSalesChannelIDs(JSONUtilities.decodeJSONArray(jsonRoot, "salesChannelIDs", true));
    if(jsonRoot.get("price") != null){
      this.price = new OfferPrice((JSONObject)jsonRoot.get("price"));
    }

  }
  
  /*****************************************
  *
  *  decodeSalesChannelIDs
  *
  *****************************************/

  private List<String> decodeSalesChannelIDs(JSONArray jsonArray) throws GUIManagerException
  {
    List<String> result = new ArrayList<String>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add((String)jsonArray.get(i));
          }
      }
    return result;
  }
  
  public static void main(String[] args) throws org.json.simple.parser.ParseException, GUIManagerException
  {
    
    // get the user input Stream
    Scanner in = new Scanner(System.in);
    System.out.println("Enter a JSON string");
    String inputJson = in.nextLine();
    
    // prepare the jsonRoot
    JSONObject jsonRoot = (JSONObject) (new JSONParser()).parse(inputJson);
    
    // use the constructor of this class to build the object based on the jsonRoot
    OfferSalesChannelsAndPrice o = new OfferSalesChannelsAndPrice(jsonRoot);
    
    // add some tests here
    // ...
    
    // now we have an object let pack it
    Object packed = OfferSalesChannelsAndPrice.pack(o);
    System.out.println("Packed Object " + packed.getClass().getName());    
    
    // now try to unpack it 
    OfferSalesChannelsAndPrice o2 = OfferSalesChannelsAndPrice.unpack(new SchemaAndValue(schema, packed));

    // do some tests here
    // ...
    
    System.out.println("O2 " + o2);    
  }
  
  
}
