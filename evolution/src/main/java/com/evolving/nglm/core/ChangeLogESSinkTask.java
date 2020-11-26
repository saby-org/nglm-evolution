/****************************************************************************
*
*  class ChangeLogESSinkTask
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.sink.SinkRecord;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;

import com.evolving.nglm.evolution.GUIManagedObject;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class ChangeLogESSinkTask<T> extends SimpleESSinkTask
{
  /*****************************************
   *
   * abstract
   *
   *****************************************/

  public abstract T unpackRecord(SinkRecord sinkRecord);

  public abstract String getDocumentID(T item);

  public abstract Map<String, Object> getDocumentMap(T item);

  /*****************************************
   *
   * getDocumentIndexName This function can be override if one wants to insert
   * documents in specific indexes when the name of the index is determined
   * given information found in the document itself.
   * 
   * The reason behind this mechanism is because pipelines (as used in
   * StreamESSinkTask) are not available with UpdateRequest and therefore can
   * not be used in ChangeLogESSinkTask.
   * 
   * It can also be useful in StreamESSinkTask when pipelines do not provide a
   * good solution.
   *
   *****************************************/

  protected String getDocumentIndexName(T item)
  {
    return this.getDefaultIndexName();
  }

  /*****************************************
   *
   * getRequests
   *
   *****************************************/

  @Override
  public List<DocWriteRequest> getRequests(SinkRecord sinkRecord)
  {
    if (sinkRecord.value() != null)
      {
        T item = unpackRecord(sinkRecord);
        if (item != null)
          {
            if (item instanceof GUIManagedObject)
              {
                GUIManagedObject guiManagedObject = (GUIManagedObject) item;
                if (guiManagedObject.getDeleted())
                  {
                    DeleteRequest deleteRequest = new DeleteRequest(getDocumentIndexName(item), getDocumentID(item));
                    deleteRequest.id(getDocumentID(item));
                    return Collections.<DocWriteRequest>singletonList(deleteRequest);

                  }
                else
                  {
                    UpdateRequest request = new UpdateRequest(getDocumentIndexName(item), getDocumentID(item));
                    request.doc(getDocumentMap(item));
                    request.docAsUpsert(true);
                    request.retryOnConflict(4);
                    return Collections.<DocWriteRequest>singletonList(request);
                  }
              }
            else
              {
                UpdateRequest request = new UpdateRequest(getDocumentIndexName(item), getDocumentID(item));
                request.doc(getDocumentMap(item));
                request.docAsUpsert(true);
                request.retryOnConflict(4);
                return Collections.<DocWriteRequest>singletonList(request);
              }

          }
      }

    return Collections.<DocWriteRequest>emptyList();
  }
}
