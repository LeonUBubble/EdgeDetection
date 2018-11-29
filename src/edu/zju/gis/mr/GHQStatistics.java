package edu.zju.gis.mr;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.vividsolutions.jts.geom.Geometry;

import edu.zju.gis.bean.MyFeature;
import edu.zju.gis.bean.Point;
import edu.zju.gis.bean.Rectangle;
import edu.zju.gis.cache.Cache;
import edu.zju.gis.utils.FormatUtils;
import edu.zju.gis.utils.GeoJsonUtils;
import edu.zju.gis.utils.GeometryUtils;
import edu.zju.gis.utils.ReadConfigUtils;


/**
 * 简单的统计，单纯地统计项目用地内的规划土地占用情况
 * @author hyr
 *
 */
public class GHQStatistics {
	
	private static Rectangle provinceRectangle = null;//浙江省的Rectangle 
	private static MyFeature ghqFeature = null;//规划区的要素
	/**
	 * 创建hbase配置
	 */
	public static Configuration config = null;
	public static Connection connection = null;
	static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", ReadConfigUtils.readWholeProperties("config/zookeeper"));
		config.set("hbase.zookeeper.property.clientPort", "2181");
		try {
			connection = ConnectionFactory.createConnection(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 表信息
	 */
	public static final String tableName0 = "IndexTable";//表名1
	public static final String tableName0_colf = "FeatureFamaily";//列族
	public static final String tableName1 = "FeatureTable";//表名1
	public static final String tableName1_colf = "GeometryFamily";//列族
	public static final String tableName1_colf_pro1 = "Geojson";//表名1-列名1
	public static final String tableName1_colf_pro2 = "Wkt";//表名1-列名2
	public static final String tableName2 = "StaticTable";//表名3  结果统计表
	public static final String tableName2_colf = "StaticFamily";//列族
	public static final String tableName2_colf_pro = "area";//表名3-列名
	public static final String tableName3 = "GHQTable";//项目用地表
	public static final String tableName3_colf = "GeometryFamily";
	public static final String tableName3_colf_pro = "Wkt";
	public static final String ghdltbCode = "2003020210";//规划地类图斑要素代码
	
	/**
	 * 初始化表结构(临时表)，及其数据
	 */
	public static void initTB() {
		HBaseAdmin admin=null;
		try {
			admin = new HBaseAdmin(config);//创建表管理
			/*删除表*/
			if (admin.tableExists(tableName2)) {
				System.out.println(tableName2 + " is already exists!");
				admin.disableTable(tableName2);
				admin.deleteTable(tableName2);
			}
			if (admin.tableExists(tableName3)) {
				System.out.println(tableName3 + " is already exists!");
				admin.disableTable(tableName3);
				admin.deleteTable(tableName3);
			}
			/*创建表*/
			HTableDescriptor desc = new HTableDescriptor(tableName2);
			HColumnDescriptor family = new HColumnDescriptor(tableName2_colf);
			desc.addFamily(family);
			admin.createTable(desc);
			System.out.println("创建表"+ tableName2 +"成功！");
			desc = new HTableDescriptor(tableName3);
			family = new HColumnDescriptor(tableName3_colf);
			desc.addFamily(family);
			admin.createTable(desc);
			System.out.println("创建表"+ tableName3 +"成功！");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static class StatisticsMapper extends TableMapper<Text, DoubleWritable>{
		private static Table ghqTable = null;
		private static Table featureTable = null;
		
		//Called once at the beginning of the task.即初始化
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			if(connection == null) {
				connection = ConnectionFactory.createConnection(config);
			}
			featureTable = connection.getTable(TableName.valueOf(tableName1));	
			ghqTable = connection.getTable(TableName.valueOf(tableName3));
			
			if(ghqFeature == null) {
				Get get = new Get("1".getBytes());
				Result result = ghqTable.get(get);
				String ghqWkt = Bytes.toString(result.getValue(tableName3_colf.getBytes(), tableName3_colf_pro.getBytes()));
				try {
					ghqFeature = new MyFeature();
					ghqFeature.wkt = ghqWkt;
					ghqFeature.envelop = GeometryUtils.createEnvelope(ghqFeature.wkt);
					ghqFeature.rectangle = new Rectangle(ghqFeature.envelop);
					ghqFeature.geometry = GeometryUtils.createGeometry(ghqWkt);
					System.out.println("ghqFeature create successfully>>>"+ghqWkt);
				} catch (Exception e) {
					e.printStackTrace();
				}		
			}
			super.setup(context);
		}
		
		private static final String PREFIX = "1";//前缀——默认和GHQTable的rowkey相同吧。

		private static Text planLandType = new Text();
		private static DoubleWritable area = new DoubleWritable();//该土地类型的面积
		@SuppressWarnings("deprecation")
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			CellScanner cellScanner = value.cellScanner();
			while(cellScanner.advance()) {
				Cell cell = cellScanner.current();
				String featureRowkey = Bytes.toString(cell.getValue());//判断是否是土地规划用地类型的图层数据
				if(featureRowkey.substring(12, 22).equals(ghdltbCode)) {
					Get get = new Get(cell.getValue());
					Result featureResult = featureTable.get(get);
					String geojson = new String(featureResult.getValue(Bytes.toBytes(tableName1_colf), Bytes.toBytes(tableName1_colf_pro1)),"utf-8");		
					String landType = GeoJsonUtils.getPropertyValue(geojson,GeoJsonUtils.PROPERTIES,"GHDLMC");
					String wkt = Bytes.toString(featureResult.getValue(Bytes.toBytes(tableName1_colf), Bytes.toBytes(tableName1_colf_pro2)));
					try {
						Geometry featureGeometry = GeometryUtils.createGeometry(wkt);
						if(featureGeometry.intersects(ghqFeature.geometry)) {
							//加适当的prefix_行政单位为统计单元的就加行政区划代码
							String mapKey = PREFIX + landType;
							//
							planLandType.set(mapKey);
							area.set(featureGeometry.intersection(ghqFeature.geometry).getArea());
							context.write(planLandType, area);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}	
				}
			}
		}
	}
	
	public static class StatisticsReducer extends TableReducer<Text, DoubleWritable, ImmutableBytesWritable>{
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			//对面积求和
			double sum = 0;
			for (DoubleWritable val : values) {//叠加
				sum += val.get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			// 封装数据
			put.addColumn(Bytes.toBytes(tableName2_colf), Bytes.toBytes(tableName2_colf_pro),Bytes.toBytes(""+sum));
			//写到hbase,需要指定rowkey、put
			context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
		}
	}
	
	public static void initGHQ() {
		//获得浙江省的外包矩形
		String provinceWkt = "POLYGON ((40304819.23290604 3005514.0952151217, 40304819.23290604 3452222.997718748, 40705006.92309791 3452222.997718748, 40705006.92309791 3005514.0952151217, 40304819.23290604 3005514.0952151217))";
		Geometry provinceGeometry = null;
		try {
			provinceGeometry = GeometryUtils.createEnvelope(provinceWkt);
			provinceRectangle = new Rectangle(provinceGeometry);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//规划区数据
		String ghqWkt = ReadConfigUtils.readSingleFeatureShape("config/singleFeatureShape");
		try {
			ghqFeature = new MyFeature();
			ghqFeature.wkt = ghqWkt;
			ghqFeature.envelop = GeometryUtils.createEnvelope(ghqFeature.wkt);
			ghqFeature.rectangle = new Rectangle(ghqFeature.envelop);
			ghqFeature.geometry = GeometryUtils.createGeometry(ghqWkt);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//读完规划区数据储存进入Hbase，为mapper阶段调用
		Table ghqTable = null;
		try {
			ghqTable = connection.getTable(TableName.valueOf(tableName3));
			Put put = new Put("1".getBytes());
			put.addColumn(tableName3_colf.getBytes(), tableName3_colf_pro.getBytes(), ghqWkt.getBytes());
			ghqTable.put(put);
			ghqTable.close();
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
	}
	
	public static void main(String[] args) throws Exception {
		initTB();
		initGHQ();
		
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
		Job job = Job.getInstance(config);//job
		job.setJar("G:/linux/ghq.jar");	
		TableMapReduceUtil.initTableMapperJob(tableName0, scan, StatisticsMapper.class,Text.class, DoubleWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(tableName2, StatisticsReducer.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	

	public static void caculatorFilterRowkey(List<Filter> filterlist) throws Exception {
		double x_incident = (provinceRectangle.p2.x - provinceRectangle.p1.x)/Cache.col;
		double y_incident = (provinceRectangle.p2.y - provinceRectangle.p1.y)/Cache.row;
		int col_start = (int) ((ghqFeature.rectangle.p1.x - provinceRectangle.p1.x)/x_incident);
		int col_end = (int) ((ghqFeature.rectangle.p2.x - provinceRectangle.p1.x)/x_incident);
		int row_start = (int) ((ghqFeature.rectangle.p1.y - provinceRectangle.p1.y)/y_incident);
		int row_end = (int) ((ghqFeature.rectangle.p2.y - provinceRectangle.p1.y)/y_incident);
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
		int col_start = (int) ((ghqFeature.rectangle.p1.x -gridRectangle.p1.x)/x_incident);
		int col_end = (int) ((ghqFeature.rectangle.p2.x - gridRectangle.p1.x)/x_incident);
		int row_start = (int) ((ghqFeature.rectangle.p1.y - gridRectangle.p1.y)/y_incident);
		int row_end = (int) ((ghqFeature.rectangle.p2.y - gridRectangle.p1.y)/y_incident);
		
		for(int x=col_start;x<=col_end;x++) {
			for(int y=row_start;y<=row_end;y++) {
				System.out.println("level="+level + " col="+ x + " row="+y);
				Rectangle grid = new Rectangle(
						new Point(gridRectangle.p1.x + x*x_incident,gridRectangle.p1.y + y*y_incident),
						new Point(gridRectangle.p1.x + (x+1)*x_incident,gridRectangle.p1.y + (y+1)*y_incident));
				grid.createEnvelop();
				if(grid.envelop.intersects(ghqFeature.geometry)) {
					RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
							new RegexStringComparator(gridRectangle.morton+level+FormatUtils.leftZeroArrang(8, x, y)));
					filterlist.add(filter);
					System.out.println("haha");
				}
			}
		}
	}

}
