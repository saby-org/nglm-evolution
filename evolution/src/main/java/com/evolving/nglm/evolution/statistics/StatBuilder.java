package com.evolving.nglm.evolution.statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatBuilder<T extends Stat<T>> {

	// a reference to the stats instance
	private T stats;
	// put a list of linked builder if needed to follow this one (to have several type linked together)
	private List<StatBuilder<?>> linkedChildBuilders;

	// the free labels changing with builder
	private Map<String,String> labels;

	protected StatBuilder(T stats){
		this.stats=stats;
		this.labels=new HashMap<>();
	}

	// always copy to keep parent builder not changed
	private StatBuilder<T> copy(){
		StatBuilder<T> toRet = new StatBuilder<>(this.stats);
		toRet.labels=new HashMap<>(this.labels);
		toRet.linkedChildBuilders=this.linkedChildBuilders;// not a copy
		return toRet;
	}

	// to keep 2 builder sync
	private void syncWith(StatBuilder<?> buider){
		this.labels=buider.labels;
	}

	public StatBuilder<T> withLabel(String labelName, String labelValue){
		StatBuilder<T> toRet = this.copy();
		toRet.labels.put(labelName,labelValue);
		return toRet;
	}

	protected void addLinkedBuilder(StatBuilder<?> builder){
		if(linkedChildBuilders==null) linkedChildBuilders = new ArrayList<>();
		linkedChildBuilders.add(builder);
	}

	private void syncLinkedBuilders(){
		if(linkedChildBuilders!=null){
			linkedChildBuilders.stream().forEach(builder->builder.syncWith(this));
		}
	}

	public T getStats(){
		syncLinkedBuilders();
		return stats.getInstance(stats,labels);
	}

}
