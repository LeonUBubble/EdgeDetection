package edu.zju.gis.mr;


import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
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
import org.apache.hadoop.hbase.client.ResultScanner;
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
import org.geotools.geojson.geom.GeometryJSON;
import org.json.JSONObject;

import com.vividsolutions.jts.geom.Geometry;

import edu.zju.gis.bean.MyFeature;
import edu.zju.gis.bean.Point;
import edu.zju.gis.bean.Rectangle;
import edu.zju.gis.cache.Cache;
import edu.zju.gis.utils.FormatUtils;
import edu.zju.gis.utils.GeoJsonUtils;
import edu.zju.gis.utils.GeometryUtils;
import edu.zju.gis.utils.HbaseUtils;
import edu.zju.gis.utils.ReadConfigUtils;

/**
 * 统计规划项目中哪些是符合规划开发的，并统计类别和面积
 * 统计规划项目中哪些是不符合规划开发的（禁止建设区），并统计类别的面积
 * @author hyr
 *
 */
public class GHQStatistics3 {
	
	public static Rectangle provinceRectangle = null;//浙江省的Rectangle 
	public static MyFeature ghqFeature = null;//规划区的要素
	
	//规划区存储的rowkey的编号（十分重要），影响很多结果表以及结果表的rowkey
	//若是行政单位的shape，则ghqStoreCode设置为行政区划代码
	public static final String ghqStoreCode = "1";
	
	public static final String ghdltbCode = "2003020210";//规划地类图斑要素代码
	public static final String gzqCode = "2003020420";//建设用地管制区编码
	public static final String regex_gzqType1 = "04[0-9]";//管制区类型代码1——禁止建设用地正则表达式
	
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
	public static final String tableName0 = "IndexTable";//空间索引表（核心数据源）
	public static final String tableName0_colf = "FeatureFamaily";//列族
	public static final String tableName1 = "FeatureTable";//规划用地表（核心数据源）
	public static final String tableName1_colf = "GeometryFamily";
	public static final String tableName1_colf_pro1 = "Geojson";
	public static final String tableName1_colf_pro2 = "Wkt";
	public static final String tableName2 = "StaticTable";//结果统计表
	public static final String tableName2_colf = "StaticFamily";
	public static final String tableName2_colf_pro = "area";
	public static final String tableName3 = "GHQTable";//规划区数据表
	public static final String tableName3_colf = "GeometryFamily";
	public static final String tableName3_colf_pro = "Wkt";
	public static final String tableName4 = "LimitedTable";//建设用地管制区数据融合表
	public static final String tableName4_colf = "GeometryFamily";
	public static final String tableName4_colf_pro = "Wkt";
	public static Table indexTable = null;
	public static Table featureTable = null;
	public static Table limitedTable = null;
	public static Table limitedFeatureTable = null;
	public static Table ghqTable = null;
	/**
	 * 初始化表结构(临时表)，及其数据
	 */
	@SuppressWarnings("deprecation")
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
			if (admin.tableExists(tableName4)) {
				System.out.println(tableName4 + " is already exists!");
				admin.disableTable(tableName4);
				admin.deleteTable(tableName4);
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
			
			desc = new HTableDescriptor(tableName4);
			family = new HColumnDescriptor(tableName4_colf);
			desc.addFamily(family);
			admin.createTable(desc);
			System.out.println("创建表"+ tableName4 +"成功！");
			
			//连接表
			indexTable = connection.getTable(TableName.valueOf(tableName0));
			featureTable = connection.getTable(TableName.valueOf(tableName1));
			ghqTable = connection.getTable(TableName.valueOf(tableName3));
			limitedTable = connection.getTable(TableName.valueOf(tableName4));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static class StatisticsMapper extends TableMapper<Text, DoubleWritable>{
		private static Table ghqTable = null;
		private static Table featureTable = null;
		private static Table limitedTable = null;
		private static MyFeature ghqFeature = null;
		private static Geometry gzqGeometry = null;
		
		//Called once at the beginning of the task.即初始化
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			if(connection == null) {
				connection = ConnectionFactory.createConnection(config);
				System.out.println("connection not created, is creating!!!");
			}
			featureTable = connection.getTable(TableName.valueOf(tableName1));	
			ghqTable = connection.getTable(TableName.valueOf(tableName3));
			limitedTable = connection.getTable(TableName.valueOf(tableName4));
			
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
			
			if(gzqGeometry == null) {
				Get get = new Get("1".getBytes());
				Result result = limitedTable.get(get);
				String gzqWkt = Bytes.toString(result.getValue(tableName3_colf.getBytes(), tableName3_colf_pro.getBytes()));
				try {
					gzqGeometry = GeometryUtils.createGeometry(gzqWkt);
					System.out.println("gzqGemetry create successfully>>>"+gzqGeometry.toString());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			super.setup(context);
		}
		
		private static final String ACCORD_PREFIX = "1";//符合规划用地
		private static final String INACCORD_PREFIX = "0";//违反规划用地
		private static Text planLandType = new Text();
		private static DoubleWritable area = new DoubleWritable();//该土地类型的面积
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			CellScanner cellScanner = value.cellScanner();
			while(cellScanner.advance()) {
				Cell cell = cellScanner.current();
				String featureRowKey = Bytes.toString(cell.getValue());
				String featureType = featureRowKey.substring(12, 22);//获取土地类型编号
				if(ghdltbCode.equals(featureType)) {//如果是规划地类图版数据
					Get get = new Get(cell.getValue());
					Result featureResult = featureTable.get(get);
					String geojson = new String(featureResult.getValue(tableName1_colf.getBytes(), tableName1_colf_pro1.getBytes()),"utf-8");
					String wkt = Bytes.toString(featureResult.getValue(tableName1_colf.getBytes(), tableName1_colf_pro2.getBytes()));
					Geometry featureGeometry = GeometryUtils.createGeometry(wkt);
					if(featureGeometry.intersects(ghqFeature.geometry)) {//与规划区相交
						if(!ghqFeature.geometry.contains(featureGeometry)) {//不是包含关系的话
							featureGeometry = featureGeometry.intersection(ghqFeature.geometry);
						}
						//替换geojson的数据
						JSONObject jsonObject = GeoJsonUtils.replaceJsonValue(geojson, "geometry", featureGeometry.toString());
						geojson = jsonObject.toString();
						String landType = GeoJsonUtils.getPropertyValue(geojson,GeoJsonUtils.PROPERTIES,"GHDLMC");
						String rowKey = null;
						if(featureGeometry.intersects(gzqGeometry)) {//与管制区相交
							rowKey = ghqStoreCode + INACCORD_PREFIX + landType;
							if(!gzqGeometry.contains(featureGeometry)) {//部分在管制区内，部分在管制区外
								//部分在管制区外处理
								Geometry featureGeometryOutgzq = featureGeometry.difference(gzqGeometry);
								String rowKey2 = ghqStoreCode + ACCORD_PREFIX + landType;
								planLandType.set(rowKey2);
								area.set(featureGeometryOutgzq.getArea());
								context.write(planLandType, area);
								//部分在管制区内处理
								featureGeometry = featureGeometry.intersection(gzqGeometry);
								if(featureGeometry.getArea()<0.01) {//如果面积太小，这是由于多个图层切割造成的（处在图层边界线上），就忽略不计了
									continue;
								}
								//替换geojson的数据
								JSONObject jsonObject2 = GeoJsonUtils.replaceJsonValue(geojson, "geometry", featureGeometry.toString());
								geojson = jsonObject2.toString();
							}
							planLandType.set(rowKey);
							area.set(featureGeometry.getArea());
							context.write(planLandType, area);
						}else {
							rowKey = ghqStoreCode + ACCORD_PREFIX + landType;
							planLandType.set(rowKey);
							area.set(featureGeometry.getArea());
							context.write(planLandType, area);
						}
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
			Put put = new Put(key.toString().getBytes("utf-8"));
			// 封装数据
			put.addColumn(Bytes.toBytes(tableName2_colf), Bytes.toBytes(tableName2_colf_pro),Bytes.toBytes(""+sum));
			//写到hbase,需要指定rowkey、put
			context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
		}
	}
	
	/**将规划区的数据存储到Hbase中,并初始化规划区的数据和浙江省的数据**/
	public static void initGHQ() {
		//获得浙江省的外包矩形，并初始化浙江省的数据
		String provinceWkt = "POLYGON ((40304819.23290604 3005514.0952151217, 40304819.23290604 3452222.997718748, 40705006.92309791 3452222.997718748, 40705006.92309791 3005514.0952151217, 40304819.23290604 3005514.0952151217))";
		Geometry provinceGeometry = null;
		try {
			provinceGeometry = GeometryUtils.createEnvelope(provinceWkt);
			provinceRectangle = new Rectangle(provinceGeometry);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//读取规划区数据，并进行初始化
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
		try {
			Put put = new Put(ghqStoreCode.getBytes());
			put.addColumn(tableName3_colf.getBytes(), tableName3_colf_pro.getBytes(), ghqWkt.getBytes());
			ghqTable.put(put);
			ghqTable.close();
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
	}
	
	public static void main(String[] args) throws Exception {
		initTB();//初始化表
		initGHQ();//初始化规划区数据和系统数据
		gzqIntergration();//规划区内禁止建设区数据数据进行融合
		
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
		job.setJar("G:/linux/ghq2.jar");	
		//MapReduce计算操作
		TableMapReduceUtil.initTableMapperJob(tableName0, scan, StatisticsMapper.class,Text.class, DoubleWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(tableName2, StatisticsReducer.class, job);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**规划区内禁止建设区数据数据进行融合**/
	public static void gzqIntergration() throws Exception {
		Scan scan = new Scan();
		List<Filter> filterlist = new ArrayList<Filter>();
		caculatorFilterRowkey(filterlist);//计算空间格网
		FilterList rowfilterList = new FilterList(Operator.MUST_PASS_ONE, filterlist);
		scan.setFilter(rowfilterList);
		ResultScanner scanner = indexTable.getScanner(scan);
		Iterator<Result> iterator = scanner.iterator();
		Geometry gzqGeometry = null;
		while(iterator.hasNext()) {
			Result result = iterator.next();
			CellScanner cellScanner = result.cellScanner();
			while(cellScanner.advance()) {
				Cell cell = cellScanner.current();
				String featureRowKey = Bytes.toString(cell.getValue());
				String featureTypeRowKey = featureRowKey.substring(12, 22);//获取土地类型编号
				if(gzqCode.equals(featureTypeRowKey)) {//如果是管制区土地
					Get get = new Get(cell.getValue());
					Result featureResult = featureTable.get(get);
					String geojson = new String(featureResult.getValue(tableName1_colf.getBytes(), tableName1_colf_pro1.getBytes()),"utf-8");
					String gzqType = GeoJsonUtils.getPropertyValue(geojson,GeoJsonUtils.PROPERTIES,"GZQLXDM");
					if(gzqType!=null && gzqType.matches(regex_gzqType1)) {
						String wkt = Bytes.toString(featureResult.getValue(tableName1_colf.getBytes(), tableName1_colf_pro2.getBytes()));
						Geometry featureGeometry = GeometryUtils.createGeometry(wkt);
						if(featureGeometry.intersects(ghqFeature.geometry)) {//禁止建设用地于规划区相交(包括包含关系)
							if(!ghqFeature.geometry.contains(featureGeometry)) {
								featureGeometry = featureGeometry.intersection(ghqFeature.geometry);
							}
							if(gzqGeometry == null) {
								gzqGeometry = featureGeometry;
							}else {
								gzqGeometry = gzqGeometry.union(featureGeometry);
							}
						}			
					}
				}
			}
		}
		//将融合而成的禁止建设区数据插入到hbase表中
		Put put = new Put(ghqStoreCode.getBytes());//rowkey等于规划区数据的rowkey
		put.addColumn(tableName4_colf.getBytes(), tableName4_colf_pro.getBytes(), gzqGeometry.toString().getBytes());
		limitedTable.put(put);
		limitedTable.close();
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
				}
			}
		}
	}

}
