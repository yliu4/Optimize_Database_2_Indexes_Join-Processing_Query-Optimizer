package simpledb.index.extensiblehash;

import java.util.ArrayList;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class LocalDepthIndex implements Index{
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private TableScan ts = null;
	private Constant searchkey = null;
	private int SizeLimit = 100;
	
	public LocalDepthIndex(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
		}
	
	public void setSearchkey(Constant searchkey) {
		this.searchkey = searchkey;
	}

	@Override
	public void beforeFirst(Constant searchkey) {
	}
	public void beforeFirst(int localval){
		close();
		int bucket = localval;
		String tblname = idxname + bucket;
		TableInfo ti = new TableInfo(tblname, sch);
		ts = new TableScan(ti, tx);
	} 
	@Override
	public boolean next() {
		while (ts.next())
			if (ts.getVal("dataval").equals(searchkey))
				return true;
		return false;
	}
	@Override
	public RID getDataRid() {
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}
	@Override
	public void insert(Constant dataval, RID rid) {
		ts.insert();
		ts.setInt("block", rid.blockNumber());
		ts.setInt("id", rid.id());
		ts.setVal("dataval", dataval);
		
	}
	@Override
	public void delete(Constant dataval, RID rid) {
		while(next())
			if (getDataRid().equals(rid)) {
				ts.delete();
				return;
			}
	}
	@Override
	public void close() {
		if (ts != null)
			ts.close();
	}
	
	public boolean IsFull(){
		boolean flag = false;
		int count = 0;
		while (ts.next()) {
			count++;
		}
		if (count >= this.SizeLimit){
			flag = true;
		}
		return flag;
	}
	public TableScan getTs() {
		return ts;
	}
	
	public ArrayList<Constant> LocalValue() {
		ArrayList<Constant> value= new ArrayList<Constant>();
		ts.beforeFirst();
		while(ts.next()) {
			value.add(ts.getVal("dataval"));
		}
		return value;
	}
	public ArrayList<RID> Rid() {
		ArrayList<RID> rids= new ArrayList<RID>();
		ts.beforeFirst();
		while(ts.next()) {
			rids.add(getDataRid());
		}
		return rids;
	}
}
