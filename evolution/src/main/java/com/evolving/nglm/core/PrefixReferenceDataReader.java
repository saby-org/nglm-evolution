/****************************************************************************
*
*  ReferenceDataReader.java
*
****************************************************************************/

package com.evolving.nglm.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PrefixReferenceDataReader<V extends ReferenceDataValue<String>> extends ReferenceDataReader<String,V>
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private volatile TrieNode trie;

  /*****************************************
  *
  *  startReader
  *
  *****************************************/

  public static <V extends ReferenceDataValue<String>> PrefixReferenceDataReader<V> startPrefixReader(String readerName, String bootstrapServers, String referenceDataTopic, UnpackValue<V> unpackValue) {
    return (PrefixReferenceDataReader<V>)startReader(readerName,bootstrapServers,referenceDataTopic,unpackValue);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  private PrefixReferenceDataReader(String readerName, String readerKey, String bootstrapServers, String referenceDataTopic, UnpackValue<V> unpackValue)
  {
    super(readerName, readerKey, bootstrapServers, referenceDataTopic, unpackValue);
    this.trie = new TrieNode("");
  }

  @Override protected void put(String key, V value){
    super.put(key,value);
    updateTrie(key,false);
  }

  @Override protected void remove(String key){
    super.remove(key);
    updateTrie(key,true);
  }

  private void updateTrie(String key, boolean toDelete)
  {
    //
    //  walk
    //

    TrieNode node = trie;
    StringBuilder prefix = new StringBuilder();
    for (int i=0; i<key.length(); i++)
      {
        Character character = key.charAt(i);
        prefix.append(character);
        TrieNode nodeForCharacter = node.getChildren().get(character);
        if (nodeForCharacter == null)
          {
            nodeForCharacter = new TrieNode(prefix.toString());
            node.getChildren().put(character, nodeForCharacter);
          }
        node = nodeForCharacter;
      }

    //
    //  update
    //

    if (!toDelete)
      node.markInSet(true);
    else
      node.markInSet(false);
  }

  /*****************************************
  *
  *  get
  *
  *****************************************/
  
  @Override public V get(String key)
  {
    //
    // short circuit on null/zerolength
    //
    
    if (key == null || key.length() == 0) return null;

    //
    //  normal case
    //

    V result = trie.getInSet() ? super.get(trie.getPrefix()) : null;
    TrieNode node = trie;
    for (int i=0; i<key.length(); i++)
      {
        //
        //  child for next characater
        //

        Character character = key.charAt(i);
        node = node.getChildren().get(character);

        //
        //  break if we have walked to the end
        //

        if (node == null)
          {
            break;
          }

        //
        //  update result
        //

        if (node.getInSet())
          {
            V candidate = super.get(node.getPrefix());
            result = (candidate != null) ? candidate : result;
          }
      }
    return result;
  }

  /*****************************************
  *
  *  class TrieNode
  *
  *****************************************/

  public static class TrieNode
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private volatile boolean inSet;
    private volatile String prefix;
    private ConcurrentMap<Character,TrieNode> children;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public TrieNode(String prefix)
    {
      this.inSet = false;
      this.prefix = prefix;
      this.children = new ConcurrentHashMap<>();
    }

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public boolean getInSet() { return inSet; }
    public String getPrefix() { return prefix; }
    public Map<Character,TrieNode> getChildren() { return children; }

    /*****************************************
    *
    *  setter
    *
    *****************************************/

    public void markInSet(boolean inSet) { this.inSet = inSet; }
  }
}
