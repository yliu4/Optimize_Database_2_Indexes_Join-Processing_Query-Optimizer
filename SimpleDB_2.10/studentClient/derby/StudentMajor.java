import java.sql.*;
import org.apache.derby.jdbc.ClientDriver;

public class StudentMajor {
    public static void main(String[] args) {
		Connection conn = null;
		try {
			// Step 1: connect to database server
			Driver d = new ClientDriver();
			String url = "jdbc:derby://localhost/studentdb;create=false";
			conn = d.connect(url, null);

			// Step 2: execute the query
			Statement stmt = conn.createStatement();
			String qry = "select SName, DName "
			           + "from DEPT, STUDENT "
			           + "where MajorId = DId";
			ResultSet rs = stmt.executeQuery(qry);

			// Step 3: loop through the result set
			System.out.println("Name\tMajor");
			while (rs.next()) {
				String sname = rs.getString("SName");
				String dname = rs.getString("DName");
				System.out.println(sname + "\t" + dname);
			}
			rs.close();
		}
		catch(SQLException e) {
			e.printStackTrace();
		}
		finally {
			// Step 4: close the connection
			try {
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
