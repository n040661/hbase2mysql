package hbaseToMysql;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class BootFrequencyFiller extends Filler {
	private static String hiveTableName = "tmp_boot_table";
	private static String hbaseTableNamePrefix = "BOOT_";
	private static String mysqlTableNamePostfix = "_start_frequency";

	public String getYearMonth() {
		return "201610"; // not complete yet
	};

	public String getYear() {
		return "2016"; // not complete yet
	}

	public String createHiveTableSql() {
		String sql = "create external table " + hiveTableName;
		sql += " (key string, boot_date string, CA_NO string) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'  WITH SERDEPROPERTIES";
		sql += " (\"hbase.columns.mapping\" = \":key,col:time, col:CA_NO\")";
		sql += " TBLPROPERTIES (\"hbase.table.name\" = \"";
		sql += hbaseTableNamePrefix + getYearMonth() + "\")";
		return sql;
	};

	public String createHiveSql() {
		String sql = "select to_date(from_unixtime(unix_timestamp(boot_date, 'yyyy.MM.dd HH:mm:ss'))), "
				+ "count(distinct CA_NO) from " + hiveTableName
				+ " group by to_date(from_unixtime(unix_timestamp(boot_date, 'yyyy.MM.dd HH:mm:ss')))";
		return sql;
	}

	public boolean createTmpHiveTable() {
		Statement stmt;
		try {
			stmt = getHiveStatement();
			String sql = "drop table if exists " + hiveTableName;
			stmt.execute(sql);
			sql = createHiveTableSql();
			stmt.execute(sql);
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public ResultSet hiveResult() {
		if (!createTmpHiveTable()) {
			return null;
		}
		Statement stmt;
		try {
			stmt = getHiveStatement();
			ResultSet rs = stmt.executeQuery(createHiveSql());
			return rs;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public boolean insertToMysql() {
		try {
			ResultSet res = hiveResult();
			Connection con = getMysqlConnection();
			String tableName = getYear() + mysqlTableNamePostfix;
			String sql = "insert into " + tableName + " (boxnumber, date, ymd, hours, number)"
					+ " values (1, '2016-01-01 01:00:00', ?, '01:00:00', ?)";

			PreparedStatement stmt = con.prepareStatement(sql);
			while (res.next()) {
				String date = res.getString(1);
				String num = res.getString(2);
				stmt.setString(1, date);
				stmt.setString(2, num);
				stmt.executeUpdate();
				System.out.println(date+ "\t" + num);
			}
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static void main(String[] args) {
		BootFrequencyFiller f = new BootFrequencyFiller();
		System.out.println(f.hbaseDriverName);
		System.out.println(f.createHiveTableSql());
		System.out.println(f.createHiveSql());
		System.out.println(f.insertToMysql());

	}
}
