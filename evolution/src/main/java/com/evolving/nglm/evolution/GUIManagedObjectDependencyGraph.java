package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.evolving.nglm.core.ServerRuntimeException;

public class GUIManagedObjectDependencyGraph
{

  //
  // data
  //

  private final int noOfNodes;
  private final List<List<Integer>> adj;

  //
  // constructor
  //

  public GUIManagedObjectDependencyGraph(Map<String, GUIDependencyModelTree> guiManagedObjectDependencyTreeMap)
  {
    this.noOfNodes = guiManagedObjectDependencyTreeMap.size();
    adj = new ArrayList<List<Integer>>(noOfNodes);
    for (int i = 0; i < noOfNodes; i++)
      {
        adj.add(new LinkedList<Integer>());
      }
    
    //
    //  build
    //
    
    build(guiManagedObjectDependencyTreeMap);
  }

  private void addEdge(int source, int dest)
  {
    adj.get(source).add(dest);
  }

  private void build(Map<String, GUIDependencyModelTree> guiManagedObjectDependencyTreeMap)
  {
    Map<String, Integer> nameIdMapping = new HashMap<String, Integer>();
    int i = 0;
    for (String key : guiManagedObjectDependencyTreeMap.keySet())
      {
        nameIdMapping.put(key, i++);
      }

    //
    // build
    //

    /*
     * for (String from : guiManagedObjectDependencyTreeMap.keySet()) { for (String
     * to : guiManagedObjectDependencyTreeMap.get(from).getDependencyList()) { if
     * (nameIdMapping.get(to) == null) throw new
     * ServerRuntimeException("invalid object type in guiManagedObjectDependencyTree "
     * + to + " in " + from); addEdge(nameIdMapping.get(from),
     * nameIdMapping.get(to)); } }
     */
  }

  public boolean isCyclic()
  {
    boolean[] visited = new boolean[noOfNodes];
    boolean[] recStack = new boolean[noOfNodes];
    for (int i = 0; i < noOfNodes; i++)
      {
        if (isCyclicUtil(i, visited, recStack))
          return true;
      }
    return false;
  }

  private boolean isCyclicUtil(int i, boolean[] visited, boolean[] recStack)
  {
    if (recStack[i]) return true;
    if (visited[i]) return false;

    visited[i] = true;
    recStack[i] = true;
    
    List<Integer> children = adj.get(i);
    for (Integer c : children)
      {
        if (isCyclicUtil(c, visited, recStack)) return true;
      }
    recStack[i] = false;
    return false;
  }

}
