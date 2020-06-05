/****************************************************************************
*
*  class ChangeLogESSinkTask
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.sink.SinkRecord;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.update.UpdateRequest;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class ChangeLogESSinkTask extends SimpleESSinkTask
{
  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  public abstract String getDocumentID(SinkRecord sinkRecord);
  public abstract Map<String,Object> getDocumentMap(SinkRecord sinkRecord);
  
  /*****************************************
  *
  *    getDocumentIndexName
  *  This function can be override if one wants to insert documents in specific indexes 
  *  when the name of the index is determined given information found in the document itself. 
  *    
  *    The reason behind this mechanism is because pipelines (as used in StreamESSinkTask) are not
  *  available with UpdateRequest and therefore can not be used here in ChangeLogESSinkTask.
  *
  *****************************************/
  
  protected String getDocumentIndexName(SinkRecord sinkRecord)
  {
    return this.getIndexName();
  }

  /*****************************************
  *
  *  getRequests
  *
  *****************************************/

  @Override public List<DocWriteRequest> getRequests(SinkRecord sinkRecord)
  {
    if (sinkRecord.value() != null && getDocumentMap(sinkRecord) != null)
      {
        UpdateRequest request = new UpdateRequest(getDocumentIndexName(sinkRecord), getDocumentID(sinkRecord));
        request.doc(getDocumentMap(sinkRecord));
        request.docAsUpsert(true);
        request.retryOnConflict(4);
        return Collections.<DocWriteRequest>singletonList(request);
      }
    else
      {
        return Collections.<DocWriteRequest>emptyList();
      }
  }
}
