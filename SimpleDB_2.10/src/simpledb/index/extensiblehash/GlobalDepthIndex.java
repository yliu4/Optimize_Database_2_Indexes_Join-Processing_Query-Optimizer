package simpledb.index.extensiblehash;

import java.util.ArrayList;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class GlobalDepthIndex implements Index{
	private LocalDepthIndex ldi;
	private Schema GlobalWithLocalDepthsch;
	private Schema GlobalWithLocalIndexsch;
	private String tn1;
	private String tn2;
	private Transaction tx;
	private TableScan ts = null;
	private Constant searchkey = null;
	private boolean isinsert = true;
	
	public GlobalDepthIndex(String idxname, Schema sch, Transaction tx) {
		System.out.println("create a global index");
		this.ldi = new LocalDepthIndex(idxname, sch, tx);
		this.tx = tx;
		this.tn1 = "GlobalWithLocalDepth";
		this.tn2 = "GlobalWithLocalIndex";
		this.GlobalWithLocalDepthsch = new Schema();
		GlobalWithLocalDepthsch.addIntField("GlobalIndex");
		GlobalWithLocalDepthsch.addIntField("LocalDepth");
		this.GlobalWithLocalIndexsch = new Schema();
		GlobalWithLocalIndexsch.addIntField("GlobalIndex");
		GlobalWithLocalIndexsch.addIntField("LocalIndex");
		TableInfo ti = new TableInfo(tn1, GlobalWithLocalDepthsch);
		ts = new TableScan(ti, tx);
		ts.insert();
		ts.setInt("GlobalIndex", 0);
		ts.setInt("LocalDepth", 0);
		ts.close();
		ti = new TableInfo(tn2, GlobalWithLocalIndexsch);
		ts = new TableScan(ti, tx);
		ts.insert();
		ts.setInt("GlobalIndex", 0);
		ts.setInt("LocalIndex", 0);
		ts.close();
		}
	
	@Override
	public void beforeFirst(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		int globaldepth = GetGlobalDepth();
		int globalindex = (int) (searchkey.hashCode() % Math.pow(2, globaldepth));
		System.out.println("The global index is " + globalindex+".");
		int localindex = Integer.MIN_VALUE;
		TableInfo ti = new TableInfo(tn1, GlobalWithLocalIndexsch);
		ts = new TableScan(ti, tx);
		while (ts.next()) {
			if (ts.getInt("GlobalIndex") == globalindex) {
				localindex = ts.getInt("LocalIndex");
			}
		}
		ts.close();
		System.out.println("The local index is " + localindex+".");
		if(ldi.IsFull()&&this.isinsert){
			System.out.println("We want to insert, however, bucket for this local index is full.");
			int localdepth = 0;
			ti = new TableInfo(tn1, GlobalWithLocalDepthsch);
			ts = new TableScan(ti, tx);
			ts.beforeFirst();//ts.beforeFirst();
			while (ts.next()) {
				if (ts.getInt("GlobalIndex") == globalindex) {
					localdepth = ts.getInt("LocalDepth");
				}
			}
			if (localdepth == globaldepth){
				System.out.println("Need to increse global depth.");
				System.out.println("Old global depth: "+this.GetGlobalDepth());
				DoubleGlobalIndex();
				System.out.println("New global depth: "+this.GetGlobalDepth());
			}
			System.out.println("split the bucket.");
			localindex = split(localindex);
			System.out.println("Then the searchkey goto bucket for local index: "+localindex);
		}
		ldi.setSearchkey(searchkey);
		ldi.beforeFirst(localindex);
	}

	@Override
	public boolean next() {
		// do not need here
		return false;
	}

	@Override
	public RID getDataRid() {
		// do not need here
		return null;
	}

	@Override
	public void insert(Constant dataval, RID datarid) {
		System.out.println("-----INSERT data"+dataval+"RID: "+datarid+"------" );
		this.isinsert = true;
		beforeFirst(dataval);
		ldi.insert(dataval,datarid);
		
	}

	@Override
	public void delete(Constant dataval, RID datarid) {
     	System.out.println("-----DELETE data"+dataval+"RID: "+datarid+"------" );
		this.isinsert = false;
		beforeFirst(dataval);
		ldi.delete(dataval,datarid);
	}
	
	@Override
	public void close() {
		if (ts != null)
			ts.close();
	}
	
	public int GetGlobalDepth(){
		TableInfo ti = new TableInfo(tn1, GlobalWithLocalDepthsch);
		TableScan ts = new TableScan(ti, tx);
		int cnt = 0;
		while (ts.next()) {
			cnt++;
		}
		ts.close();
		return (int) (Math.log(cnt) / Math.log(2));
	}
	public void DoubleGlobalIndex(){
		int maxglobalval = (int) Math.pow(2, GetGlobalDepth());
		for (int i = 0; i < maxglobalval; i++){
			TableInfo ti = new TableInfo(tn1, GlobalWithLocalDepthsch);
			TableScan ts = new TableScan(ti, tx);
			int newglobalindex = i+maxglobalval;
			int olddepth = Integer.MIN_VALUE;
			while (ts.next()) {
				if (ts.getInt("GlobalIndex") == i) {
					olddepth = ts.getInt("LocalDepth");
				}
			}
			ts.insert();
			ts.setInt("GlobalIndex", newglobalindex);
			ts.setInt("LocalDepth", olddepth);
			ts.close();
			ti = new TableInfo(tn1, GlobalWithLocalIndexsch);
			ts = new TableScan(ti, tx);
			int oldlocalindex = Integer.MIN_VALUE;
			while (ts.next()) {
				if (ts.getInt("GlobalIndex") == i) {
					oldlocalindex = ts.getInt("LocalIndex");
				}
			}
			ts.insert();
			ts.setInt("GlobalIndex", newglobalindex);
			ts.setInt("LocalIndex", oldlocalindex);
			ts.close();
		}
	}
	private int split(int localindex) {
		int newindex =(int) (localindex + Math.pow(2, GetGlobalDepth()-1));
		ldi.beforeFirst(localindex);
		ArrayList<Constant> localval = ldi.LocalValue();
		ArrayList<RID> rid = ldi.Rid();
		int numofrecord = rid.size();
		int depth = Integer.MIN_VALUE;
		ldi.getTs().beforeFirst();//ts.beforefirst
		for (int i = 0; i<numofrecord; i++){
			//ts.beforefirst
			delete(localval.get(i),rid.get(i));	
		}
		//ts.beforefirst
		TableInfo ti = new TableInfo(tn1, GlobalWithLocalDepthsch);
		TableScan ts = new TableScan(ti, tx);
		
		while (ts.next()) {
			if (ts.getInt("GlobalIndex") == newindex) {
				depth = ts.getInt("LocalDepth");
				ts.delete();
				break;
			}
		}
		//ts.beforefirst
		while (ts.next()) {
			if (ts.getInt("GlobalIndex") == localindex) {
				ts.delete();
				break;
			}
		}
		ts.insert();
		ts.setInt("GlobalIndex", newindex);
		ts.setInt("LocalDepth", depth+1);
		ts.setInt("GlobalIndex", localindex);
		ts.setInt("LocalDepth", depth+1);
		ts.close();
		ti = new TableInfo(tn1, GlobalWithLocalIndexsch);
		ts = new TableScan(ti, tx);
		while (ts.next()) {
			if (ts.getInt("GlobalIndex") == newindex) {
				ts.delete();
			}
		}
		ts.insert();
		ts.setInt("GlobalIndex", newindex);
		ts.setInt("LocalIndex", newindex);
		ts.close();
		for (int i = 0; i<numofrecord; i++){
			insert(localval.get(i),rid.get(i));	
		}
		int globaldepth = GetGlobalDepth();
		int globalindex = (int) (searchkey.hashCode() % Math.pow(2, globaldepth));
		int local = Integer.MIN_VALUE;
		ti = new TableInfo(tn1, GlobalWithLocalIndexsch);
		ts = new TableScan(ti, tx);
		while (ts.next()) {
			if (ts.getInt("GlobalIndex") == globalindex) {
				local = ts.getInt("LocalIndex");
			}
		}
		ts.close();
		return local;
	}
	
	

}
