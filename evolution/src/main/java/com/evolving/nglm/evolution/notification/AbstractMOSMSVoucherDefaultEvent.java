package com.evolving.nglm.evolution.notification;

import com.evolving.nglm.core.*;
import com.evolving.nglm.evolution.*;
import com.evolving.nglm.evolution.preprocessor.PreprocessorContext;
import com.evolving.nglm.evolution.preprocessor.PreprocessorEvent;
import org.apache.kafka.connect.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;

public abstract class AbstractMOSMSVoucherDefaultEvent implements EvolutionEngineEvent, MONotificationEvent, PreprocessorEvent
{

  // abstract methods
  public abstract boolean init(PreprocessorContext context);//lets child implementation init stuff with context if needed
  public abstract String getFinalUserSubscriberIDOrNull();// not mandatory but if child implementation can provide, it is better (avoid any lookup)
  public abstract Pair<AlternateID,String> getFinalUserAlternateIDOrNull();// not mandatory but if child implementation can provide, it is better (redis lookup instead of ES lookup)
  public abstract String getVoucherCodeFromNotification();// can as well be null if not MO SMS meant to act on voucher
  public abstract String getSupplierDisplayFromNotification();// needed to be returned if you want a subscriberID lookup based on voucherCode (voucher code is unique per supplierID)

  // schema
  private static Schema schema = null;
  static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("MOSMSVoucherDefaultEvent");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("eventDate", Timestamp.SCHEMA);
      schemaBuilder.field("channelName", Schema.STRING_SCHEMA);
      schemaBuilder.field("sourceAddress", Schema.STRING_SCHEMA);
      schemaBuilder.field("destinationAddress", Schema.STRING_SCHEMA);
      schemaBuilder.field("messageText", Schema.STRING_SCHEMA);
      schemaBuilder.field("isTransferred", Schema.OPTIONAL_BOOLEAN_SCHEMA);
      schemaBuilder.field("voucherCode", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("supplierDisplay", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("senderSubscriberID", Schema.OPTIONAL_STRING_SCHEMA);
      schema = schemaBuilder.build();
    }

  public static Schema superschema() { return schema; }

  private static final Logger log = LoggerFactory.getLogger(AbstractMOSMSVoucherDefaultEvent.class);

  private String subscriberID;
  private Date originTimesTamp;
  private String channelName;
  private String sourceAddress;
  private String destinationAddress;
  private String messageText;
  Boolean isTransferred;
  String voucherCode;
  String supplierDisplay;
  String senderSubscriberID;

  // when nothing to return after preprocess
  private boolean noResult=false;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  @Override public String getSubscriberID() { return subscriberID; }
  @Override public Date getEventDate() { return originTimesTamp; }
  public String getChannelName() { return channelName; }
  public String getSourceAddress() { return sourceAddress; }
  public String getDestinationAddress() { return destinationAddress; }
  public String getMessageText() { return messageText; }
  public Boolean isTransferred(){ return isTransferred; }
  public String getSenderSubscriberID() { return senderSubscriberID; }
  public String getVoucherCode() { return voucherCode; }
  public String getSupplierDisplay() { return supplierDisplay; }

  @Override public DeliveryRequest.DeliveryPriority getDeliveryPriority(){return DeliveryRequest.DeliveryPriority.High; }

  // empty constructor, needed cause empty constructor needed in child impl for reflection instance creation in smpp plugin
  public AbstractMOSMSVoucherDefaultEvent() {}

  // unpack constructor
  public AbstractMOSMSVoucherDefaultEvent(SchemaAndValue schemaAndValue)
  {

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();

    Struct valueStruct = (Struct) value;
    this.subscriberID = valueStruct.getString("subscriberID");
    this.originTimesTamp = (Date) valueStruct.get("eventDate");
    this.channelName = valueStruct.getString("channelName");
    this.sourceAddress = valueStruct.getString("sourceAddress");
    this.destinationAddress = valueStruct.getString("destinationAddress");
    this.messageText = valueStruct.getString("messageText");
    this.isTransferred = valueStruct.getBoolean("isTransferred");
    this.voucherCode = valueStruct.getString("voucherCode");
    this.senderSubscriberID = valueStruct.getString("senderSubscriberID");
    this.supplierDisplay = valueStruct.getString("supplierDisplay");
  }

  public static Object superpack(Object value)
  {
    AbstractMOSMSVoucherDefaultEvent event = (AbstractMOSMSVoucherDefaultEvent) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", event.getSubscriberID());
    struct.put("eventDate", event.getEventDate());
    struct.put("channelName", event.getChannelName());
    struct.put("sourceAddress", event.getSourceAddress());
    struct.put("destinationAddress", event.getDestinationAddress());
    struct.put("messageText", event.getMessageText());
    if(event.isTransferred()!=null) struct.put("isTransfered", event.isTransferred());
    if(event.getVoucherCode()!=null) struct.put("voucherCode", event.getVoucherCode());
    if(event.getSenderSubscriberID()!=null) struct.put("senderSubscriberID", event.getSenderSubscriberID());
    if(event.getSupplierDisplay()!=null) struct.put("supplierDisplay", event.getSupplierDisplay());
    return struct;
  }
 
  @Override
  public void fillWithMOInfos(String subscriberID, Date originTimesTamp, String channelName, String sourceAddress, String destinationAddress, String messageText)
  {
    this.subscriberID = subscriberID;
    this.originTimesTamp = originTimesTamp;
    this.channelName = channelName;
    this.sourceAddress = sourceAddress;
    this.destinationAddress = destinationAddress;
    this.messageText = messageText;
  }

  @Override
  public void preprocessEvent(PreprocessorContext context) {

    if(!this.init(context)){
      this.noResult=true;
      return;
    }

    if(this.getVoucherCodeFromNotification()==null) return;//nothing to do
    this.voucherCode=this.getVoucherCodeFromNotification();
    this.supplierDisplay=this.getSupplierDisplayFromNotification();

    this.isTransferred=false;// we will have to take care when will be true

    // subscriberId could be provided by the child impl
    if(getFinalUserSubscriberIDOrNull()!=null){
      assignSubscriberID(getFinalUserSubscriberIDOrNull());
      return;
    }

    // alternateId could be provided by the child impl, redis lookup
    if(this.getFinalUserAlternateIDOrNull()!=null){
      String alternateIDName = this.getFinalUserAlternateIDOrNull().getFirstElement().getID();
      String alternateID = this.getFinalUserAlternateIDOrNull().getSecondElement();
      String subscriberId = null;
      try{
        subscriberId = context.getSubscriberIDService().getSubscriberIDBlocking(alternateIDName,alternateID);
      }catch (SubscriberIDService.SubscriberIDServiceException e){
        log.info("AbstractMOSMSVoucherDefaultEvent.preprocessEvent : issue resolving alternateId "+alternateIDName+" "+alternateID,e);
      }
      assignSubscriberID(subscriberId);
      return;
    }

    // only voucherCode/supplierID are provided, ES lookup
    // supplierID is needed
    if(this.supplierDisplay==null){
      log.info("AbstractMOSMSVoucherDefaultEvent.preprocessEvent : no supplierDisplay returned");
      this.noResult=true;
      return;
    }

    // need supplier ID from display
    String supplierID=null;
    for(Supplier supplier:context.getSupplierService().getActiveSuppliers(SystemTime.getCurrentTime())){
      if(this.supplierDisplay.equals(supplier.getGUIManagedObjectDisplay())){
        supplierID=supplier.getSupplierID();
      }
    }
    if(supplierID==null){
      log.info("AbstractMOSMSVoucherDefaultEvent.preprocessEvent : no configuration for supplier "+this.supplierDisplay);
      this.noResult=true;
      return;
    }

    VoucherPersonalES esVoucher = VoucherPersonalESService.getESVoucherFromVoucherCode(supplierID,this.getVoucherCodeFromNotification(),context.getElasticsearchClientAPI());
    if(esVoucher==null){
      log.info("AbstractMOSMSVoucherDefaultEvent.preprocessEvent : no voucher "+this.subscriberID+" - "+this.getVoucherCodeFromNotification()+" stored in ES");
      this.noResult=true;
      return;
    }
    assignSubscriberID(esVoucher.getSubscriberId());

    // IF YOU WANT THIS CHECK (voucher transferred comply with conf), IT HAS TO BE DONE IN "CUSTO CODE" (this class being considered as custo code helper)
    if(this.isTransferred()){
      Voucher voucher = context.getVoucherService().getActiveVoucher(esVoucher.getVoucherId(),SystemTime.getCurrentTime());
      if(voucher==null){
        log.info("AbstractMOSMSVoucherDefaultEvent.preprocessEvent : bad voucher configuration for ID "+esVoucher.getVoucherId()+", ignoring event "+this.getVoucherCode());
        noResult=true;
        return;
      }
      VoucherType voucherType = context.getVoucherTypeService().getActiveVoucherType(voucher.getVoucherTypeId(),SystemTime.getCurrentTime());
      if(voucherType==null){
        log.info("AbstractMOSMSVoucherDefaultEvent.preprocessEvent : bad voucherType configuration for ID "+voucher.getVoucherTypeId()+", ignoring event "+this.getVoucherCode());
        noResult=true;
        return;
      }
      if(!voucherType.getTransferable()){
        log.info("AbstractMOSMSVoucherDefaultEvent.preprocessEvent : received voucher MO from "+this.getSenderSubscriberID()+" while owned by "+this.getSubscriberID()+" on non transferable voucher "+voucher.getVoucherDisplay()+", ignoring event "+this.getVoucherCode());
        noResult=true;
        return;
      }

    }

  }

  @Override
  public Collection<EvolutionEngineEvent> getPreprocessedEvents() {
    if(noResult) return Collections.emptyList();
    return Collections.singleton(this);
  }

  private void assignSubscriberID(String subscriberID){
    if(subscriberID==null){
      noResult=true;
      return;
    }
    if(!subscriberID.equals(this.getSubscriberID())){
      this.isTransferred = true;
      this.senderSubscriberID=this.getSubscriberID();
    }
    this.subscriberID=subscriberID;
  }


}
