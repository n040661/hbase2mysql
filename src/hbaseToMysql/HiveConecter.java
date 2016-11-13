package hbaseToMysql;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
public class HiveConecter {
	 private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	  /**
	   * @param args
	   * @throws SQLException
	   */
	  public static void main(String[] args) throws SQLException {
	      try {
	      Class.forName(driverName);
	    } catch (ClassNotFoundException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	      System.exit(1);
	    }
	    //replace "hive" here with the name of the user the queries should run as
	    Connection con = DriverManager.getConnection("jdbc:hive2://192.168.0.120:10000/default", "hive", "");
	    Statement stmt = con.createStatement();
	    String tableName = "BOOT_201610";
	    stmt.execute("drop table if exists " + tableName);
	    String sql =   "CREATE external TABLE event_table (key string, value string) " 
	    		+ "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' "
	    		+ "WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key,colfam1:q1\") "
	    		+ "TBLPROPERTIES (\"hbase.table.name\" = \"event_table\")";
	    System.out.println(sql);
	    stmt.execute(sql);
	    
	    sql = "show tables '" + tableName + "'";
	    System.out.println("Running: " + sql);
	    ResultSet res = stmt.executeQuery(sql);
	    if (res.next()) {
	      System.out.println(res.getString(1));
	    }
	       // describe table
	    sql = "describe " + tableName;
	    System.out.println("Running: " + sql);
	    res = stmt.executeQuery(sql);
	    while (res.next()) {
	      System.out.println(res.getString(1) + "\t" + res.getString(2));
	    }
	    sql = "select * from "+tableName;
	    res = stmt.executeQuery(sql);
	    while (res.next()) {
	      System.out.println(res.getString(1) + "\t" + res.getString(2));
	    }
	    stmt.execute("drop table if exists " + tableName);
	    // load data into table
	    // NOTE: filepath has to be local to the hive server
	    // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
	  }
}
