package in.pazmysaz.essql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Test;

/**
 * https://dbschema.com/jdbc-driver/Elasticsearch.html
 * @author jpvel
 *
 */
public class ElasticSearchJdbcClient {

		
	
	
	@Test
	public void testSql4ESConnection()
	{
		try {
			Class.forName("in.pazmysaz.essql.jdbc.ESDriver");
			String index = "schema_pimkpi";
			Connection conn = DriverManager.getConnection("jdbc:sql4es://172.18.0.3:9300/"+index+"?cluster.name=es-docker-cluster");
			Statement statement = conn.createStatement();
			ResultSet query = statement.executeQuery("SELECT * FROM _doc ");
			ResultSetMetaData rsmd = query.getMetaData();
			int nrCols = rsmd.getColumnCount();
			// get other column information like type
			while(query.next()){
				for(int i=1; i<=nrCols; i++){
			  		System.out.println(query.getObject(i));
				}
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSql4ESCount()
	{
		try {
			Class.forName("in.pazmysaz.essql.jdbc.ESDriver");
			String index = "schema_pimkpi";
			Connection conn = DriverManager.getConnection("jdbc:sql4es://172.18.0.3:9300/"+index+"?cluster.name=es-docker-cluster");
			Statement statement = conn.createStatement();
			ResultSet query = statement.executeQuery("SELECT COUNT(*) as count_ FROM _doc ");
			ResultSetMetaData rsmd = query.getMetaData();
			int nrCols = rsmd.getColumnCount();
			// get other column information like type
			while(query.next()){
				for(int i=1; i<=nrCols; i++){
			  		System.out.println(query.getObject(i));
				}
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void testRawConnection()
	{
		/*
		 * Settings settings = ImmutableSettings.settingsBuilder() .put("cluster.name",
		 * "foxzen") .put("node.name", "yu").build(); Client client = new
		 * TransportClient(settings) .addTransportAddress(new
		 * InetSocketTransportAddress("XXX.XXX.XXX.XXX", 9200)); // XXX is my server's
		 * ip address IndexResponse response = client.prepareIndex("twitter", "tweet")
		 * .setSource(XContentFactory.jsonBuilder() .startObject() .field("productId",
		 * "1") .field("productName", "XXX").endObject()).execute().actionGet();
		 * System.out.println(response.getIndex());
		 * System.out.println(response.getType());
		 * System.out.println(response.getVersion()); client.close();
		 */
	}
	
}
