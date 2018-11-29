package edu.zju.gis.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.vividsolutions.jts.geom.Geometry;

import edu.zju.gis.bean.MyFeature;
import edu.zju.gis.bean.Point;
import edu.zju.gis.bean.Rectangle;
import edu.zju.gis.cache.Cache;
import edu.zju.gis.utils.FormatUtils;
import edu.zju.gis.utils.GeometryUtils;
import edu.zju.gis.utils.ReadConfigUtils;


/**
 * 不要使用，有错误
 * @author hyr
 *
 */

public class GHQStatistics2_1 {
	
	private static Rectangle provinceRectangle = null;//浙江省的Rectangle 
	private static MyFeature feature = null;//规划区的要素
	/**
	 * 创建hbase配置
	 */
	static Configuration config = null;
	static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", ReadConfigUtils.readWholeProperties("config/zookeeper"));
		config.set("hbase.zookeeper.property.clientPort", "2181");
		System.out.println("init static doce====>>>>GHQStatistics2_1");
		//获得浙江省的外包矩形
		String provinceWkt = "POLYGON ((40304819.23290604 3005514.0952151217, 40304819.23290604 3452222.997718748, 40705006.92309791 3452222.997718748, 40705006.92309791 3005514.0952151217, 40304819.23290604 3005514.0952151217))";
		Geometry provinceGeometry = null;
		try {
			provinceGeometry = GeometryUtils.createEnvelope(provinceWkt);
			provinceRectangle = new Rectangle(provinceGeometry);
		} catch (Exception e) {
			e.printStackTrace();
		}
		String ghqWkt = ReadConfigUtils.readSingleFeatureShape("config/singleFeatureShape");
		feature = new MyFeature();
		feature.wkt = ghqWkt;
		try {
			feature.envelop = GeometryUtils.createEnvelope(feature.wkt);
			feature.rectangle = new Rectangle(feature.envelop);
			feature.geometry = GeometryUtils.createGeometry(ghqWkt);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 表信息
	 */
	public static final String tableName0 = "IndexTable";//表名1
	public static final String tableName0_colf = "FeatureFamaily";//列族
	public static final String tableName1 = "FeatureTempTable";//表名2
	public static final String tableName1_colf = "NullFamaily";//列族
	public static final String tableName1_colf_pro = "NullValue";//列族
	
	/**
	 * 初始化表结构(临时表)，及其数据
	 */
	public static void initTB() {
		
		HBaseAdmin admin=null;
		try {
			admin = new HBaseAdmin(config);//创建表管理
			/*删除表*/
			System.out.println(admin.tableExists(tableName1));
			if (admin.tableExists(tableName1)) {
				System.out.println(tableName1 + " is already exists!");
				admin.disableTable(tableName1);
				admin.deleteTable(tableName1);
			}
			/*创建表*/
			HTableDescriptor desc = new HTableDescriptor(tableName1);
			HColumnDescriptor family = new HColumnDescriptor(tableName1_colf);
			desc.addFamily(family);
			admin.createTable(desc);
			System.out.println("创建表"+ tableName1 +"成功！");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static class StatisticsMapper extends TableMapper<Text, Text>{
		private static Text featureRowKey = new Text();//土地要素的rowkey
		private static Text whatever = new Text();//随便输出一个东西，只是为了套用这个模型，实际没卵用
		
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			CellScanner cellScanner = value.cellScanner();
			while(cellScanner.advance()) {
				Cell cell = cellScanner.current();
				featureRowKey.set(Bytes.toString(cell.getValue()));
				context.write(featureRowKey, whatever);
			}
		}
	}
	
	public static class StatisticsReducer extends TableReducer<Text, Text, ImmutableBytesWritable>{
		private static Text whatever = new Text();//随便输出一个东西，只是为了套用这个模型，实际没卵用
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// 创建put，设置rowkey为土地要素的rowkey
			Put put = new Put(Bytes.toBytes(key.toString()));
			// 封装数据
			put.addColumn(Bytes.toBytes(tableName1_colf), Bytes.toBytes(tableName1_colf_pro),Bytes.toBytes(String.valueOf(whatever)));
			//写到hbase,需要指定rowkey、put
			context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
		}
	}
	
	public static void main(String[] args) throws Exception {
		initTB();
		
		//创建job
		Job job = Job.getInstance(config);//job
		
		job.setJar("G:/linux/ghq1.jar");		
		/**
		 * 进行空间索引的过滤操作
		 */
		Scan scan = new Scan();
		List<Filter> filterlist = new ArrayList<Filter>();
		///
		caculatorFilterRowkey(filterlist);
		///
		FilterList rowfilterList = new FilterList(Operator.MUST_PASS_ONE, filterlist);
		scan.setFilter(rowfilterList);
		TableMapReduceUtil.initTableMapperJob(tableName0, scan, StatisticsMapper.class,Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob(tableName1, StatisticsReducer.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public void executor() throws Exception {
		initTB();
		
		Iterator<Entry<String, String>> iterator = config.iterator();
		while(iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			System.out.println(entry.getKey()+":"+entry.getValue());
			
		}
		//创建job
		Job job = Job.getInstance(config);//job
		job.setJar("G:/linux/ghq.jar");		
		/**
		 * 进行空间索引的过滤操作
		 */
		Scan scan = new Scan();
		List<Filter> filterlist = new ArrayList<Filter>();
		///
		caculatorFilterRowkey(filterlist);
		///
		FilterList rowfilterList = new FilterList(Operator.MUST_PASS_ONE, filterlist);
		scan.setFilter(rowfilterList);
		TableMapReduceUtil.initTableMapperJob(tableName0, scan, StatisticsMapper.class,Text.class, Text.class, job);
		TableMapReduceUtil.initTableReducerJob(tableName1, StatisticsReducer.class, job);
		job.waitForCompletion(true);
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

	public static void caculatorFilterRowkey(List<Filter> filterlist) throws Exception {
		double x_incident = (provinceRectangle.p2.x - provinceRectangle.p1.x)/Cache.col;
		double y_incident = (provinceRectangle.p2.y - provinceRectangle.p1.y)/Cache.row;
		int col_start = (int) ((feature.rectangle.p1.x - provinceRectangle.p1.x)/x_incident);
		int col_end = (int) ((feature.rectangle.p2.x - provinceRectangle.p1.x)/x_incident);
		int row_start = (int) ((feature.rectangle.p1.y - provinceRectangle.p1.y)/y_incident);
		int row_end = (int) ((feature.rectangle.p2.y - provinceRectangle.p1.y)/y_incident);
		for(int x=col_start;x<=col_end;x++) {
			for(int y=row_start;y<=row_end;y++) {
				Point p1 = new Point(provinceRectangle.p1.x + x*x_incident, provinceRectangle.p1.y + y*y_incident);
				Point p2 = new Point(provinceRectangle.p1.x + (x+1)*x_incident, provinceRectangle.p1.y + (y+1)*y_incident);
				Rectangle gridRectangle = new Rectangle(p1, p2);
				String parentMorton = null;
				try {
					parentMorton = FormatUtils.getMorton(6, x, y);
					gridRectangle.createEnvelop();
				} catch (Exception e) {
					e.printStackTrace();
				}
				gridRectangle.morton = parentMorton;
				filterInBigGrid(filterlist,gridRectangle);
			}
		}
	}

	//在大格网中过滤rowkey
	public static void filterInBigGrid(List<Filter> filterlist,Rectangle gridRectangle) throws Exception {
		for(int level=1;level<6;level++) {
			filterInLevel(filterlist,gridRectangle,level);
		}
	}

	//在层级格网中过滤rowkey
	public static void filterInLevel(List<Filter> filterlist, Rectangle gridRectangle, int level) throws Exception {
		int row = (int) Math.pow(2, level-1);
		int col = row;
		double x_incident = (gridRectangle.p2.x - gridRectangle.p1.x)/col;
		double y_incident = (gridRectangle.p2.y - gridRectangle.p1.y)/row;
		int col_start = (int) ((feature.rectangle.p1.x -gridRectangle.p1.x)/x_incident);
		int col_end = (int) ((feature.rectangle.p2.x - gridRectangle.p1.x)/x_incident);
		int row_start = (int) ((feature.rectangle.p1.y - gridRectangle.p1.y)/y_incident);
		int row_end = (int) ((feature.rectangle.p2.y - gridRectangle.p1.y)/y_incident);
		
		for(int x=col_start;x<=col_end;x++) {
			for(int y=row_start;y<=row_end;y++) {
				System.out.println("level="+level + " col="+ x + " row="+y);
				Rectangle grid = new Rectangle(
						new Point(gridRectangle.p1.x + x*x_incident,gridRectangle.p1.y + y*y_incident),
						new Point(gridRectangle.p1.x + (x+1)*x_incident,gridRectangle.p1.y + (y+1)*y_incident));
				grid.createEnvelop();
				if(grid.envelop.intersects(feature.geometry)) {
					RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
							new RegexStringComparator(gridRectangle.morton+level+FormatUtils.leftZeroArrang(8, x, y)));
					filterlist.add(filter);
				}
			}
		}
	}
}
