package edu.zju.gis.test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
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
import edu.zju.gis.utils.ReadConfigUtils;

/**
 * �����õع��������ϳɵ�������WKT����������
 * @author hyr
 *
 */
public class GHQIntegration {

	private static Configuration config = null;
	private static Connection connection = null;
	private static Table indexTable = null;
	private static Table featureTable = null;
	private static Table limitedTable = null;
	private static Table limitedFeatureTable = null;
	private static MyFeature ghqFeature =null;
	private static Rectangle provinceRectangle = null;//�㽭ʡ��Rectangle 
	private static final String ACCORD_PREFIX = "1";
	private static final String INACCORD_PREFIX = "0";
	public static final String tableName0 = "IndexTable";//����1
	public static final String tableName0_colf = "FeatureFamaily";//����
	public static final String tableName1 = "FeatureTable";//����1
	public static final String tableName1_colf = "GeometryFamily";//����
	public static final String tableName1_colf_pro1 = "Geojson";//����1-����1
	public static final String tableName1_colf_pro2 = "Wkt";//����1-����2
	public static final String tableName2 = "LimitedTable";//����2
	public static final String tableName2_colf = "GeometryFamily";//����1
	public static final String tableName2_colf_pro = "Wkt";//����1
	public static final String tableName3 = "LimitedFeatureTable";//����2
	public static final String tableName3_colf = "GeometryFamily";//����1
	public static final String tableName3_colf_pro = "Geojson";//����1
	
	public static final String jqdlxzCode="2003010100";//������״ͼ��
	public static final String gzqCode = "2003020420";//�����õع���������
	public static final String regex_gzqType1 = "04[0-9]";//���������ʹ���1������ֹ�����õ�������ʽ
	
	
	
	public static void init() throws IOException {
		config = HBaseConfiguration.create();// ����
		config.set("hbase.zookeeper.quorum", ReadConfigUtils.readWholeProperties("config/zookeeper"));// zookeeper��ַ
		config.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper�˿�
		connection = ConnectionFactory.createConnection(config);
		indexTable = connection.getTable(TableName.valueOf(tableName0));
		featureTable = connection.getTable(TableName.valueOf(tableName1));
		//��ʼ����
		HBaseAdmin admin = new HBaseAdmin(config);//���������
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
		HTableDescriptor desc = new HTableDescriptor(tableName2);
		HColumnDescriptor family = new HColumnDescriptor(tableName2_colf);
		desc.addFamily(family);
		admin.createTable(desc);
		
		desc = new HTableDescriptor(tableName3);
		family = new HColumnDescriptor(tableName3_colf);
		desc.addFamily(family);
		admin.createTable(desc);
		limitedTable = connection.getTable(TableName.valueOf(tableName2));
		limitedFeatureTable = connection.getTable(TableName.valueOf(tableName3));
	}
	
	public static void initGHQ() {
		//����㽭ʡ���������
		String provinceWkt = "POLYGON ((40304819.23290604 3005514.0952151217, 40304819.23290604 3452222.997718748, 40705006.92309791 3452222.997718748, 40705006.92309791 3005514.0952151217, 40304819.23290604 3005514.0952151217))";
		Geometry provinceGeometry = null;
		try {
			provinceGeometry = GeometryUtils.createEnvelope(provinceWkt);
			provinceRectangle = new Rectangle(provinceGeometry);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//�滮������
		String ghqWkt = ReadConfigUtils.readSingleFeatureShape("config/singleFeatureShape");
		try {
			ghqFeature = new MyFeature();
			ghqFeature.wkt = ghqWkt;
			ghqFeature.envelop = GeometryUtils.createEnvelope(ghqFeature.wkt);
			ghqFeature.rectangle = new Rectangle(ghqFeature.envelop);
			ghqFeature.geometry = GeometryUtils.createGeometry(ghqWkt);
			System.out.println(ghqFeature.rectangle);
			System.out.println(ghqFeature.wkt);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {	
		init();
		initGHQ();
		String gzqWkt = prepare();
		mapreduce(gzqWkt);
		closeTable(indexTable,featureTable,limitedTable,limitedFeatureTable);
	}
	
	private static void closeTable(Table ... tables) {
		for (Table table : tables) {
			if(table!=null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}

	public static void mapreduce(String gzqWkt) throws Exception{
		//������洢һ��map�С���������
		Map<String, Double> areaMap = new HashMap<>();
		Scan scan = new Scan();
		List<Filter> filterlist = new ArrayList<Filter>();
		///
		caculatorFilterRowkey(filterlist);
		///
		FilterList rowfilterList = new FilterList(Operator.MUST_PASS_ONE, filterlist);
		scan.setFilter(rowfilterList);
		ResultScanner scanner = indexTable.getScanner(scan);
		Iterator<Result> iterator = scanner.iterator();
		//��������Gemotry
		Geometry gzqGeometry = GeometryUtils.createGeometry(gzqWkt);
		while(iterator.hasNext()) {
			Result result = iterator.next();
			CellScanner cellScanner = result.cellScanner();
			while(cellScanner.advance()) {				
				Cell cell = cellScanner.current();
				String featureRowKey = Bytes.toString(cell.getValue());
				String featureType = featureRowKey.substring(12, 22);//��ȡ�������ͱ��
				if(jqdlxzCode.equals(featureType)) {//�������״ͼ
					Get get = new Get(cell.getValue());
					Result featureResult = featureTable.get(get);
					String geojson = Bytes.toString(featureResult.getValue(tableName1_colf.getBytes(), tableName1_colf_pro1.getBytes()));
					String wkt = Bytes.toString(featureResult.getValue(tableName1_colf.getBytes(), tableName1_colf_pro2.getBytes()));
					Geometry featureGeometry = GeometryUtils.createGeometry(wkt);
					if(featureGeometry.intersects(ghqFeature.geometry)) {//��滮���ཻ
						if(!ghqFeature.geometry.contains(featureGeometry)) {
							featureGeometry = featureGeometry.intersection(ghqFeature.geometry);
						}
						//�滻geojson������
						JSONObject jsonObject = GeoJsonUtils.replaceJsonValue(geojson, "geometry", featureGeometry.toString());
						geojson = jsonObject.toString();
						
						String landType = GeoJsonUtils.getPropertyValue(geojson,GeoJsonUtils.PROPERTIES,"DLMC");
						String mapKey = null;
						if(featureGeometry.intersects(gzqGeometry)) {//��������ཻ
							mapKey = INACCORD_PREFIX + landType;
							if(!gzqGeometry.contains(featureGeometry)) {//�����ڹ������ڣ������ڹ�������
								//�����ڹ������⴦��
								Geometry featureGeometryOutgzq = featureGeometry.difference(gzqGeometry);
								String mapKey2 = ACCORD_PREFIX + landType;
								double area = 0;
								if(areaMap.containsKey(mapKey2)) {
									area = areaMap.get(mapKey2);
								}	
								area += featureGeometryOutgzq.getArea();
								areaMap.put(mapKey2, area);
								//�����ڹ������ڴ���
								featureGeometry = featureGeometry.intersection(gzqGeometry);
								if(featureGeometry.getArea()<0.01) {//������̫С���������ڶ��ͼ���и���ɵģ�����ͼ��߽����ϣ����ͺ��Բ�����
									continue;
								}
								//�滻geojson������
								
								JSONObject jsonObject2 = GeoJsonUtils.replaceJsonValue(geojson, "geometry", featureGeometry.toString());
								geojson = jsonObject2.toString();
							}
							double area = 0;
							if(areaMap.containsKey(mapKey)) {
								area = areaMap.get(mapKey);
							}	
							area += featureGeometry.getArea();
							areaMap.put(mapKey, area);
							//���������ڵ�Ҫ�ش洢��hbase��
							//��֮ǰ����һЩ����
							GeometryJSON gJson = new GeometryJSON();
							System.out.println(geojson);
							Reader reader = new StringReader(geojson);
							Geometry geometry = gJson.read(reader);
							if(geometry.difference(gzqGeometry).getArea()>2) {
								System.out.println(geojson);
							}
							//-------------
							Put put = new Put(featureRowKey.getBytes());
							put.addColumn(tableName3_colf.getBytes(), tableName3_colf_pro.getBytes(), geojson.getBytes());
							limitedFeatureTable.put(put);
						}else {
							mapKey = ACCORD_PREFIX + landType;
							double area = 0;
							if(areaMap.containsKey(mapKey)) {
								area = areaMap.get(mapKey);
							}	
							area += featureGeometry.getArea();
							areaMap.put(mapKey, area);
						}
					}
				}
			}
		}
		Set<String> keySet = areaMap.keySet();
		for (String key : keySet) {
			System.out.println(key + ":" + areaMap.get(key));
		}
	}
	
	
	public static String prepare() throws Exception {
		int count = 0;
		int intesectionCount = 0;
		Scan scan = new Scan();
		List<Filter> filterlist = new ArrayList<Filter>();
		///
		caculatorFilterRowkey(filterlist);
		///
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
					String geojson = Bytes.toString(featureResult.getValue(tableName1_colf.getBytes(), tableName1_colf_pro1.getBytes()));
					String gzqType = GeoJsonUtils.getPropertyValue(geojson,GeoJsonUtils.PROPERTIES,"GZQLXDM");
					if(gzqType!=null && gzqType.matches(regex_gzqType1)) {
						count++;
						String wkt = Bytes.toString(featureResult.getValue(tableName1_colf.getBytes(), tableName1_colf_pro2.getBytes()));
						Geometry featureGeometry = GeometryUtils.createGeometry(wkt);
						if(featureGeometry.intersects(ghqFeature.geometry)) {//��ֹ�����õ��ڹ滮���ཻ(����������ϵ)
							intesectionCount++;
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
		//���ں϶��ɵĽ�ֹ�������ݲ��뵽hbase����
		Put put = new Put("001".getBytes());
		put.addColumn(tableName2_colf.getBytes(), tableName2_colf_pro.getBytes(), gzqGeometry.toString().getBytes());
		limitedTable.put(put);
		System.out.println("count:"+count);
		System.out.println("intesectionCount:"+intesectionCount);
		System.out.println(gzqGeometry);
		return gzqGeometry.toString();
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
				Rectangle grid = new Rectangle(
						new Point(gridRectangle.p1.x + x*x_incident,gridRectangle.p1.y + y*y_incident),
						new Point(gridRectangle.p1.x + (x+1)*x_incident,gridRectangle.p1.y + (y+1)*y_incident));
				grid.createEnvelop();
				if(grid.envelop.intersects(ghqFeature.geometry)) {
					RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, 
							new RegexStringComparator(gridRectangle.morton+level+FormatUtils.leftZeroArrang(8, x, y)));
					filterlist.add(filter);
					Geometry indexGemetry = GeometryUtils.createEnvelope(new Rectangle(
							new Point(gridRectangle.p1.x + x*x_incident,gridRectangle.p1.y + y*y_incident),
							new Point(gridRectangle.p1.x + (x+1)*x_incident,gridRectangle.p1.y + (y+1)*y_incident)
							)); 
					System.out.println(level+"==>>"+indexGemetry);
				}
			}
		}
	}
}
