/****************************************************************************
*
*  class StreamESSinkTask
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.sink.SinkRecord;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class StreamESSinkTask<T> extends SimpleESSinkTask
{
  /*****************************************
  *
  *  abstract
  *
  *****************************************/
  
  public abstract T unpackRecord(SinkRecord sinkRecord);
  public abstract Map<String,Object> getDocumentMap(T t);
  
  /*****************************************
  *
  *    getDocumentIndexName
  *  This function can be override if one wants to insert documents in specific indexes 
  *  when the name of the index is determined given information found in the document itself. 
  *    
  *    The reason behind this mechanism is because pipelines (as used in StreamESSinkTask) are not
  *  available with UpdateRequest and therefore can not be used in ChangeLogESSinkTask.
  *  
  *    It can also be useful in StreamESSinkTask when pipelines do not provide a good solution.
  *
  *****************************************/
  
  protected String getDocumentIndexName(T t) 
  { 
    return this.getDefaultIndexName(); 
  }

  /*****************************************
  *
  *  getRequests
  *
  *****************************************/

  @Override public List<DocWriteRequest> getRequests(SinkRecord sinkRecord)
  {
    if (sinkRecord.value() != null) {
      T item = unpackRecord(sinkRecord);
      if (item != null) {
        IndexRequest request = new IndexRequest(getDocumentIndexName(item));
        request.source(getDocumentMap(item));
        if (! getPipelineName().equals("")) {
          request.setPipeline(getPipelineName());
        }
        return Collections.<DocWriteRequest>singletonList(request);
      }
    }
      
    return Collections.<DocWriteRequest>emptyList();
  }
}
