package hbaseToMysql;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
public class Filler {
	protected static String hbaseDriverName = "org.apache.hive.jdbc.HiveDriver";
	protected static String mysqlDriverName = "com.mysql.jdbc.Driver";
	protected static String hiveAddress = "jdbc:hive2://192.168.199.156:10000/default";
	protected static String mysqlAddress = "jdbc:mysql://192.168.199.156:3306/radiotv?user=root&password=cloudera";
	Filler(){
		try {
		      Class.forName(hbaseDriverName);
		      Class.forName(mysqlDriverName);
		    } catch (ClassNotFoundException e) {
		      // TODO Auto-generated catch block
		      e.printStackTrace();
		      System.exit(1);
		    }
	};
	public Statement getHiveStatement() throws SQLException{
		 Connection con = DriverManager.getConnection(hiveAddress, "hive", "");
		 Statement stmt = con.createStatement();
		 return stmt;
	};
	public Connection getMysqlConnection() throws SQLException{
		Connection con = DriverManager.getConnection(mysqlAddress);
		return con;
	}
	public static void main(String[] args) {
		Filler f = new Filler();
		System.out.println("asdasd");
	}

}
