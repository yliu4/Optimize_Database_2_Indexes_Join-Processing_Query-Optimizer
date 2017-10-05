package simpledb.index.test;


import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import simpledb.remote.SimpleDriver;

public class JoinTestTables {
	
	public static void main(String[] args) {
		Connection conn = null;
		Driver d = new SimpleDriver();
		String host = "localhost"; 
		String url = "jdbc:simpledb://" + host;
		Statement s = null;
		try {
			conn = d.connect(url, null);
			s = conn.createStatement();
			long startat = System.nanoTime();
			s.executeQuery("SELECT a1,a2, a3, a4 FROM test1, test5 where a1=a3;");
			long endat = System.nanoTime();
			System.out.println("static hash index based join: " + (endat-startat) + " ns");
			startat = System.nanoTime();
			s.executeQuery("SELECT a1,a2, a3, a4 FROM test2, test5 where a1=a3;");
			endat = System.nanoTime();
			System.out.println("Extensible hash index based join: " + (endat-startat) + " ns");
			startat = System.nanoTime();
			s.executeQuery("SELECT a1,a2, a3, a4 FROM test3, test5 where a1=a3;");
			endat = System.nanoTime();
			System.out.println("b-tree index based join: " + (endat-startat) + " ns");
			startat = System.nanoTime();
			s.executeQuery("SELECT a1,a2, a3, a4 FROM test4, test5 where a1=a3;");
			endat = System.nanoTime();
			System.out.println("no index based join: " + (endat-startat) + " ns");
			
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

