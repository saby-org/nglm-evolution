package com.evolving.nglm.evolution.zookeeper;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Configuration {

	// the Zookeeper nodes used by evolution
	// NOTE: nodes here are automatically created if not existing in Zookeeper, in the order they are declared
	// SO ORDER IS IMPORTANT, a child node creation will failed if parent does not exists
	public enum NODE{

		// root one, just to create them on init if needed
		EVOLVING(Node.getRootNode().newChild("evolving")),
		NGLM_ROOT(EVOLVING.node().newChild("nglm")),

		// subscriberGroups nodes TODO: clean up creation from setup
		SUBSCRIBERGROUPS(NGLM_ROOT.node().newChild("subscriberGroups")),
		SUBSCRIBERGROUPS_EPOCHS(SUBSCRIBERGROUPS.node().newChild("epochs")),
		SUBSCRIBERGROUPS_LOCKS(SUBSCRIBERGROUPS.node().newChild("locks")),

		// stock nodes TODO: clean up creation from setup
		STOCK(NGLM_ROOT.node().newChild("stock")),
		STOCK_STOCKS(STOCK.node().newChild("stocks")),
		STOCK_STOCKMONITORS(STOCK.node().newChild("stockmonitors")),

		// report nodes TODO: reports/lock seems not used, then clean it up, clean up creation from code
		REPORTS(NGLM_ROOT.node().newChild("reports")),
		REPORTS_CONTROL(REPORTS.node().newChild("control")),

		// uniqueKeyServer node TODO: clean up creation from code
		UNIQKEYSERVER(NGLM_ROOT.node().newChild("uniqueKeyServer")),

		// voucher nodes TODO: node hierarchy changed, clean up code
		VOUCHER(NGLM_ROOT.node().newChild("voucher")),
		VOUCHER_PERSONNAL_IMPORT(VOUCHER.node.newChild("voucherPersonalImport")),
		VOUCHER_PERSONNAL_JOBTOPROCESS(VOUCHER_PERSONNAL_IMPORT.node().newChild("job_to_process")),

		// locks nodes
		LOCKS(NGLM_ROOT.node.newChild("locks")),

		// propensity nodes
		PROPENSITY(NGLM_ROOT.node.newChild("propensity"));

		// licencemanager nodes
		// TODO: licensemanager I don't care for now, probably just to remove

		Node node;
		NODE(Node node){
			this.node=node;
		}
		public Node node(){return this.node;}
		@Override public String toString(){return this.name()+"("+node().toString()+")";}
	}

	// utils conf

	// the time it will take for zookeeper to consider the client "died" if no heartbeat received
	public final static int SESSION_TIMEOUT_MS = 10000;
	// the encoding charset to store data
	public final static Charset ENCODING_CHARSET = StandardCharsets.UTF_8;



}
