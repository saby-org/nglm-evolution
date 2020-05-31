/****************************************************************************
*
*  ReferenceDataReader.java
*
****************************************************************************/

package com.evolving.nglm.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PrefixReferenceDataReader<V extends ReferenceDataValue<String>> extends ReferenceDataReader<String,V>
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private TrieNode trie;

  /*****************************************
  *
  *  startReader
  *
  *****************************************/

  public static <V extends ReferenceDataValue<String>> PrefixReferenceDataReader<V> startPrefixReader(String readerName, String readerKey, String bootstrapServers, String referenceDataTopic, UnpackValue<V> unpackValue)
  {
    synchronized (readersLock)
      {
        if (! readerReferences.containsKey(readerName))
          {
            if (readers.get(readerName) != null) throw new ServerRuntimeException("invariant - readers/readReferences start 1");
            readerReferences.put(readerName, new Integer(1));
            readers.put(readerName, new PrefixReferenceDataReader<V>(readerName, readerKey, bootstrapServers, referenceDataTopic, unpackValue));
            readers.get(readerName).start();
          }
        else
          {
            if (readers.get(readerName) == null) throw new ServerRuntimeException("invariant - readers/readReferences start 2");
            readerReferences.put(readerName, new Integer(readerReferences.get(readerName).intValue() + 1));
          }
        return (PrefixReferenceDataReader<V>) readers.get(readerName);
      }
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

  /*****************************************
  *
  *  updateKey
  *
  *****************************************/

  @Override protected void updateKey(String key, Set<ReferenceDataRecord> recordsForKey)
  {
    /*****************************************
    *
    *  cases
    *
    *****************************************/

    boolean keyAdded = recordsForKey.size() > 0 && ! referenceData.containsKey(key);
    boolean keyDeleted = recordsForKey.size() == 0 && referenceData.containsKey(key);
    
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super.updateKey(key, recordsForKey);

    /*****************************************
    *
    *  updateTrie
    *
    *****************************************/

    if (keyAdded || keyDeleted)
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

        if (keyAdded)
          node.markInSet(true);
        else
          node.markInSet(false);
      }
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

    synchronized (this)
      {
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

    private boolean inSet;
    private String prefix;
    private Map<Character,TrieNode> children;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public TrieNode(String prefix)
    {
      this.inSet = false;
      this.prefix = prefix;
      this.children = new HashMap<Character,TrieNode>();
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
