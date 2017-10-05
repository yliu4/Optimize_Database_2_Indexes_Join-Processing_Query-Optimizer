package simpledb.index.test;


/******************************************************************/

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

import simpledb.remote.SimpleDriver;

public class SelectTestTables {
	
	public static void main(String[] args) throws Exception {
		
		Connection conn = null;
		Driver d = new SimpleDriver();
		String host = "localhost";	
		String url = "jdbc:simpledb://" + host;
		Statement s = null;
		try {
			conn = d.connect(url, null);
			s = conn.createStatement();
			
			// test1
			long startat = System.nanoTime();
			s.executeQuery("SELECT a1,a2 FROM test1 where a1=100;");
			long endat = System.nanoTime();
			System.out.println("static hash index based selection:\t" + (endat-startat) + " ns");
			
			// test2
			startat = System.nanoTime();
			s.executeQuery("SELECT a1,a2 FROM test2 where a1=100;");
			endat = System.nanoTime();
			System.out.println("Extensible hash index based selection:\t" + (endat-startat) + " ns");
			
			// test3
			startat = System.nanoTime();
			s.executeQuery("SELECT a1,a2 FROM test3 where a1=100;");
			endat = System.nanoTime();
			System.out.println("b-tree index based selection:\t" + (endat-startat) + " ns");
			
			// test4
			startat = System.nanoTime();
			s.executeQuery("SELECT a1,a2 FROM test4 where a1=100;");
			endat = System.nanoTime();
			System.out.println("no index based selection:\t" + (endat-startat) + " ns");
			
			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
}

