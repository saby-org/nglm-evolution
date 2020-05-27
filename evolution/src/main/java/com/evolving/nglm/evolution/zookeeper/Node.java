package com.evolving.nglm.evolution.zookeeper;

public class Node {

	// the root node
	private static Node ROOT = new Node();
	// root only available in package
	protected static Node getRootNode(){return ROOT;}

	// always the entire full path
	private String fullPath;
	// only the node name
	private String name;

	// a very special for the genesis root one only
	private Node(){
		this.fullPath="";
	}

	// private, can not get a node without parent
	private Node(String path, String node){
		this.fullPath =path+"/"+node;
		this.name=node;
	}

	// builder pattern, only way to get a node is from an other one
	public Node newChild(String node){return new Node(this.fullPath,node);}

	// the full path
	public String fullPath(){return fullPath;}
	// just the node name
	public String nodeName(){return name;}

	@Override public String toString(){return fullPath;}
}
