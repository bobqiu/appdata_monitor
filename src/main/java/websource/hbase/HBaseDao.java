package websource.hbase;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import websource.model.AppData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseDao{
	public static Configuration config;
	public static HTable table;
	public static final String HOUR_PATH = "/home/cly/package/tomcat/webapps/show_chart/hours.tsv";
	public static final String DAY_TOTAL_PATH = "/home/cly/package/tomcat/webapps/show_chart/total.tsv";
	public static final String DAY_EMPTY_PATH = "/home/cly/package/tomcat/webapps/show_chart/empty.tsv";

	public static void main(String [] args) {
		config = HBaseConfiguration.create();
		try {
			table = new HTable(config, Bytes.toBytes("appdata_monitor"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		Map<String,AppData> map = new HashMap<String,AppData>();
		List<AppData> list = new ArrayList<AppData>();
		
		Scan scan=new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(true);
		scan.setStartRow(Bytes.toBytes("2013031300"));
		scan.setStopRow(Bytes.toBytes("2013032623"));
		
		ResultScanner scanner = null;
		try {
			scanner = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(Result rs : scanner){
			// hours
			long total = 0L;
			long empty = 0L;
			String dayHour = Bytes.toString(rs.getRow());
			if (!rs.isEmpty()) {
				Map<byte[], byte[]> factors = rs.getFamilyMap(Bytes.toBytes("cf"));
				total = Bytes.toLong(factors.get("total".getBytes()));
				empty = Bytes.toLong(factors.get("empty".getBytes()));
			}
			//int total = Bytes.toInt(rs.getValue(Bytes.toBytes("cf"),Bytes.toBytes("total")));
			System.out.println(total);
			//int empty = Bytes.toInt(rs.getValue(Bytes.toBytes("cf"),Bytes.toBytes("empty")));
			System.out.println(empty);
			list.add(new AppData(dayHour, total, empty));
			
			// days
			String day = dayHour.substring(0,8);
			if (map.get(day) == null) {
				map.put(day, new AppData(day,total,empty));
			}else{
				total += map.get(day).total;
				empty += map.get(day).empty;
				map.put(day, new AppData(day,total,empty));
			}
			
		}
		// write hours
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(HOUR_PATH));
			bw.write("dayhours" + "\t" + "total" + "\t" + "empty");
			for (AppData info :list) {
				bw.newLine();
				bw.write(info.date + "\t" + info.total + "\t" + info.empty);
				bw.flush();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// init
		String [] days = {"20130313","20130314","20130315","20130316","20130317","20130318","20130319",
				"20130320","20130321","20130322","20130323","20130324","20130325","20130326"};
		long [] total = new long[14];
		long [] empty = new long[14];
		int num = 0;

		Object [] key_arr =  map.keySet().toArray();
		Arrays.sort(key_arr);
		for(Object key:key_arr){
			total[num] = map.get(String.valueOf(key)).total;
			empty[num] = map.get(String.valueOf(key)).empty;
			num ++;
		}

		// print days total
		try {
			bw = new BufferedWriter(new FileWriter(DAY_TOTAL_PATH));
			bw.write("date1" + "\t" + "total1" + "\t" + "date2" + "total2");
			for (int i = 0; i < 7; i++) {
				bw.newLine();
				bw.write(days[i] + "\t" + total[i] + "\t" + days[i+7] + "\t" + total[i+7]);
				bw.flush();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// print days empty
		try {
			bw = new BufferedWriter(new FileWriter(DAY_EMPTY_PATH));
			bw.write("date1" + "\t" + "empty1" + "\t" + "date2" + "empty2");
			for (int i = 0; i < 7; i++) {
				bw.newLine();
				bw.write(days[i] + "\t" + empty[i] + "\t" + days[i+7] + "\t" + empty[i+7]);
				bw.flush();
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
