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

public abstract class StreamESSinkTask extends SimpleESSinkTask
{
  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  public abstract Map<String,Object> getDocumentMap(SinkRecord sinkRecord);

  /*****************************************
  *
  *  getRequests
  *
  *****************************************/

  @Override public List<DocWriteRequest> getRequests(SinkRecord sinkRecord)
  {
    if (sinkRecord.value() != null && getDocumentMap(sinkRecord) != null)
      {
        IndexRequest request = new IndexRequest(getIndexName(), "_doc");
        request.source(getDocumentMap(sinkRecord));
        if (! getPipelineName().equals("")) request.setPipeline(getPipelineName());
        return Collections.<DocWriteRequest>singletonList(request);
      }
    else
      {
        return Collections.<DocWriteRequest>emptyList();
      }
  }
}
