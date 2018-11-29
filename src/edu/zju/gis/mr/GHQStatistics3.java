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
 * ͳ�ƹ滮��Ŀ����Щ�Ƿ��Ϲ滮�����ģ���ͳ���������
 * ͳ�ƹ滮��Ŀ����Щ�ǲ����Ϲ滮�����ģ���ֹ������������ͳ���������
 * @author hyr
 *
 */
public class GHQStatistics3 {
	
	public static Rectangle provinceRectangle = null;//�㽭ʡ��Rectangle 
	public static MyFeature ghqFeature = null;//�滮����Ҫ��
	
	//�滮���洢��rowkey�ı�ţ�ʮ����Ҫ����Ӱ��ܶ������Լ�������rowkey
	//����������λ��shape����ghqStoreCode����Ϊ������������
	public static final String ghqStoreCode = "1";
	
	public static final String ghdltbCode = "2003020210";//�滮����ͼ��Ҫ�ش���
	public static final String gzqCode = "2003020420";//�����õع���������
	public static final String regex_gzqType1 = "04[0-9]";//���������ʹ���1������ֹ�����õ�������ʽ
	
	/**
	 * ����hbase����
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
	 * ����Ϣ
	 */
	public static final String tableName0 = "IndexTable";//�ռ���������������Դ��
	public static final String tableName0_colf = "FeatureFamaily";//����
	public static final String tableName1 = "FeatureTable";//�滮�õر���������Դ��
	public static final String tableName1_colf = "GeometryFamily";
	public static final String tableName1_colf_pro1 = "Geojson";
	public static final String tableName1_colf_pro2 = "Wkt";
	public static final String tableName2 = "StaticTable";//���ͳ�Ʊ�
	public static final String tableName2_colf = "StaticFamily";
	public static final String tableName2_colf_pro = "area";
	public static final String tableName3 = "GHQTable";//�滮�����ݱ�
	public static final String tableName3_colf = "GeometryFamily";
	public static final String tableName3_colf_pro = "Wkt";
	public static final String tableName4 = "LimitedTable";//�����õع����������ںϱ�
	public static final String tableName4_colf = "GeometryFamily";
	public static final String tableName4_colf_pro = "Wkt";
	public static Table indexTable = null;
	public static Table featureTable = null;
	public static Table limitedTable = null;
	public static Table limitedFeatureTable = null;
	public static Table ghqTable = null;
	/**
	 * ��ʼ����ṹ(��ʱ��)����������
	 */
	@SuppressWarnings("deprecation")
	public static void initTB() {
		HBaseAdmin admin=null;
		try {
			admin = new HBaseAdmin(config);//���������
			/*ɾ����*/
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
			/*������*/
			HTableDescriptor desc = new HTableDescriptor(tableName2);
			HColumnDescriptor family = new HColumnDescriptor(tableName2_colf);
			desc.addFamily(family);
			admin.createTable(desc);
			System.out.println("������"+ tableName2 +"�ɹ���");
			
			desc = new HTableDescriptor(tableName3);
			family = new HColumnDescriptor(tableName3_colf);
			desc.addFamily(family);
			admin.createTable(desc);
			System.out.println("������"+ tableName3 +"�ɹ���");
			
			desc = new HTableDescriptor(tableName4);
			family = new HColumnDescriptor(tableName4_colf);
			desc.addFamily(family);
			admin.createTable(desc);
			System.out.println("������"+ tableName4 +"�ɹ���");
			
			//���ӱ�
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
		
		//Called once at the beginning of the task.����ʼ��
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
		
		private static final String ACCORD_PREFIX = "1";//���Ϲ滮�õ�
		private static final String INACCORD_PREFIX = "0";//Υ���滮�õ�
		private static Text planLandType = new Text();
		private static DoubleWritable area = new DoubleWritable();//���������͵����
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			CellScanner cellScanner = value.cellScanner();
			while(cellScanner.advance()) {
				Cell cell = cellScanner.current();
				String featureRowKey = Bytes.toString(cell.getValue());
				String featureType = featureRowKey.substring(12, 22);//��ȡ�������ͱ��
				if(ghdltbCode.equals(featureType)) {//����ǹ滮����ͼ������
					Get get = new Get(cell.getValue());
					Result featureResult = featureTable.get(get);
					String geojson = new String(featureResult.getValue(tableName1_colf.getBytes(), tableName1_colf_pro1.getBytes()),"utf-8");
					String wkt = Bytes.toString(featureResult.getValue(tableName1_colf.getBytes(), tableName1_colf_pro2.getBytes()));
					Geometry featureGeometry = GeometryUtils.createGeometry(wkt);
					if(featureGeometry.intersects(ghqFeature.geometry)) {//��滮���ཻ
						if(!ghqFeature.geometry.contains(featureGeometry)) {//���ǰ�����ϵ�Ļ�
							featureGeometry = featureGeometry.intersection(ghqFeature.geometry);
						}
						//�滻geojson������
						JSONObject jsonObject = GeoJsonUtils.replaceJsonValue(geojson, "geometry", featureGeometry.toString());
						geojson = jsonObject.toString();
						String landType = GeoJsonUtils.getPropertyValue(geojson,GeoJsonUtils.PROPERTIES,"GHDLMC");
						String rowKey = null;
						if(featureGeometry.intersects(gzqGeometry)) {//��������ཻ
							rowKey = ghqStoreCode + INACCORD_PREFIX + landType;
							if(!gzqGeometry.contains(featureGeometry)) {//�����ڹ������ڣ������ڹ�������
								//�����ڹ������⴦��
								Geometry featureGeometryOutgzq = featureGeometry.difference(gzqGeometry);
								String rowKey2 = ghqStoreCode + ACCORD_PREFIX + landType;
								planLandType.set(rowKey2);
								area.set(featureGeometryOutgzq.getArea());
								context.write(planLandType, area);
								//�����ڹ������ڴ���
								featureGeometry = featureGeometry.intersection(gzqGeometry);
								if(featureGeometry.getArea()<0.01) {//������̫С���������ڶ��ͼ���и���ɵģ�����ͼ��߽����ϣ����ͺ��Բ�����
									continue;
								}
								//�滻geojson������
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
			//��������
			double sum = 0;
			for (DoubleWritable val : values) {//����
				sum += val.get();
			}
			Put put = new Put(key.toString().getBytes("utf-8"));
			// ��װ����
			put.addColumn(Bytes.toBytes(tableName2_colf), Bytes.toBytes(tableName2_colf_pro),Bytes.toBytes(""+sum));
			//д��hbase,��Ҫָ��rowkey��put
			context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
		}
	}
	
	/**���滮�������ݴ洢��Hbase��,����ʼ���滮�������ݺ��㽭ʡ������**/
	public static void initGHQ() {
		//����㽭ʡ��������Σ�����ʼ���㽭ʡ������
		String provinceWkt = "POLYGON ((40304819.23290604 3005514.0952151217, 40304819.23290604 3452222.997718748, 40705006.92309791 3452222.997718748, 40705006.92309791 3005514.0952151217, 40304819.23290604 3005514.0952151217))";
		Geometry provinceGeometry = null;
		try {
			provinceGeometry = GeometryUtils.createEnvelope(provinceWkt);
			provinceRectangle = new Rectangle(provinceGeometry);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//��ȡ�滮�����ݣ������г�ʼ��
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
		//����滮�����ݴ������Hbase��Ϊmapper�׶ε���
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
		initTB();//��ʼ����
		initGHQ();//��ʼ���滮�����ݺ�ϵͳ����
		gzqIntergration();//�滮���ڽ�ֹ�������������ݽ����ں�
		
		/**
		 * ���пռ������Ĺ��˲���
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
		//MapReduce�������
		TableMapReduceUtil.initTableMapperJob(tableName0, scan, StatisticsMapper.class,Text.class, DoubleWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(tableName2, StatisticsReducer.class, job);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**�滮���ڽ�ֹ�������������ݽ����ں�**/
	public static void gzqIntergration() throws Exception {
		Scan scan = new Scan();
		List<Filter> filterlist = new ArrayList<Filter>();
		caculatorFilterRowkey(filterlist);//����ռ����
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
				String featureTypeRowKey = featureRowKey.substring(12, 22);//��ȡ�������ͱ��
				if(gzqCode.equals(featureTypeRowKey)) {//����ǹ���������
					Get get = new Get(cell.getValue());
					Result featureResult = featureTable.get(get);
					String geojson = new String(featureResult.getValue(tableName1_colf.getBytes(), tableName1_colf_pro1.getBytes()),"utf-8");
					String gzqType = GeoJsonUtils.getPropertyValue(geojson,GeoJsonUtils.PROPERTIES,"GZQLXDM");
					if(gzqType!=null && gzqType.matches(regex_gzqType1)) {
						String wkt = Bytes.toString(featureResult.getValue(tableName1_colf.getBytes(), tableName1_colf_pro2.getBytes()));
						Geometry featureGeometry = GeometryUtils.createGeometry(wkt);
						if(featureGeometry.intersects(ghqFeature.geometry)) {//��ֹ�����õ��ڹ滮���ཻ(����������ϵ)
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
		//���ں϶��ɵĽ�ֹ���������ݲ��뵽hbase����
		Put put = new Put(ghqStoreCode.getBytes());//rowkey���ڹ滮�����ݵ�rowkey
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

	//�ڴ�����й���rowkey
	public static void filterInBigGrid(List<Filter> filterlist,Rectangle gridRectangle) throws Exception {
		for(int level=1;level<6;level++) {
			filterInLevel(filterlist,gridRectangle,level);
		}
	}

	//�ڲ㼶�����й���rowkey
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
