package in.pazmysaz.essql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.sort.SortOrder;

import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;

import in.pazmysaz.essql.jdbc.ESStatement;
import in.pazmysaz.essql.model.*;
import in.pazmysaz.essql.model.Column.Operation;
import in.pazmysaz.essql.model.expression.IComparison;
import in.pazmysaz.essql.parse.se.SearchAggregationParser;
import in.pazmysaz.essql.parse.se.SearchHitParser;
import in.pazmysaz.essql.parse.sql.ParseResult;
import in.pazmysaz.essql.parse.sql.QueryParser;

/**
 * This class maintains the state of a {@link ESStatement} and is used interpret SELECT statements,
 * execute and parse them and keep {@link ResultSet} state while doing so.
 *  
 * @author cversloot
 *
 */
public class ESQueryState{

	// relevant resources
	private final QueryParser parser = new QueryParser();
	private final Client client;
	private final Properties props;
	private final Statement statement;
	private final SearchHitParser hitParser = new SearchHitParser();
	private final SearchAggregationParser aggParser = new SearchAggregationParser();
	
	// state definition
	private SearchRequestBuilder request;
	private ESResultSet result = null;
	private SearchResponse esResponse;
	private Heading heading = new Heading();;
	private IComparison having = null;
	private List<OrderBy> orderings = new ArrayList<OrderBy>();
	
	private int limit = -1;
	private boolean splitRS;
	private int fetchSize;
	private int maxRowsRS = Integer.MAX_VALUE;
	

	/**
	 * Creates a QueryState using the specified client. This involves retrieving index and type information
	 * from Elasticsearch.
	 * @param client
	 * @param statement
	 * @throws SQLException
	 */
	public ESQueryState(Client client, Statement statement) throws SQLException{
		this.client = client;
		this.statement = statement;
		this.props = statement.getConnection().getClientInfo();
		this.splitRS = Utils.getBooleanProp(props, Utils.PROP_RESULTS_SPLIT, false);
		this.fetchSize = Utils.getIntProp(props, Utils.PROP_FETCH_SIZE, 10000);
		if(splitRS) maxRowsRS = fetchSize;
	}
	
	/**
	 * Builds the Elasticsearch query to be executed on the specified indexes. This function refreshes the 
	 * state after which it is not possible to retrieve results for any previously build queries. 
	 * @param sql
	 * @param indices
	 * @return 
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	public void buildRequest(String sql, QueryBody query, String... indices) throws SQLException {
		if(this.esResponse != null && this.esResponse.getScrollId() != null){
			client.prepareClearScroll().addScrollId(this.esResponse.getScrollId()).execute();
		}
		// ToDo: Check indices after parse

		Map<String, Map<String, Integer>> esInfo = (Map<String, Map<String, Integer>>)Utils.getObjectProperty(props, Utils.PROP_TABLE_COLUMN_MAP);
		ParseResult parseResult =  parser.parse(sql, query, maxRowsRS, this.statement.getConnection().getClientInfo(), esInfo);
		List<String> sqlIndices = new ArrayList<>();
		//List<String> types = new ArrayList<>();
		for (QuerySource source : parseResult.getSources()) {
		    if (source.getIndex() != null) {
                sqlIndices.add(source.getIndex());
            }
            //types.add(source.getSource());
        }
        if (sqlIndices.isEmpty()) {
            sqlIndices.addAll(Arrays.asList(indices));
        }
        this.request = client.prepareSearch(sqlIndices.toArray(new String[0]));

		buildQuery(request, parseResult);
		this.heading = parseResult.getHeading();
		having = parseResult.getHaving();
		orderings = parseResult.getSorts();
		this.limit = parseResult.getLimit();
		
		// add highlighting
		// ToDo: fix highlighting
/*		for(Column column : heading.columns()){
			if(column.getOp() == Operation.HIGHLIGHT){
				request.addHighlightedField(column.getColumn(), Utils.getIntProp(props, Utils.PROP_FRAGMENT_SIZE, 100), 
						Utils.getIntProp(props, Utils.PROP_FRAGMENT_NUMBER, 1));
			}
		}*/
	}
	
	/**
	 * Builds the Elasticsearch query object based on the parsed information from the SQL query
	 * @param searchReq
	 * @param info
	 */
	private void buildQuery(SearchRequestBuilder searchReq, ParseResult info) {
		String[] types = new String[info.getSources().size()];
		for(int i=0; i<info.getSources().size(); i++) types[i] = info.getSources().get(i).getSource(); 
		SearchRequestBuilder req = searchReq.setTypes(types);
		
		// add filters and aggregations
		if(info.getAggregation() != null){
			// when aggregating the query must be a query and not a filter
			if(info.getQuery() != null)	req.setQuery(info.getQuery());
			req.addAggregation(info.getAggregation());
			
		// ordering does not work on aggregations (has to be done in client)
		}else if(info.getQuery() != null){
			if(info.getRequestScore()) req.setQuery(info.getQuery()); // use query instead of filter to get a score
			else req.setPostFilter(info.getQuery());
			
			// add order
			for(OrderBy ob : info.getSorts()){
				req.addSort(ob.getField(), ob.getOrder());
			}
		} else req.setQuery(QueryBuilders.matchAllQuery());
		
		this.limit = info.getLimit();
		if(splitRS) maxRowsRS = fetchSize;
		
		//System.out.println("fetch: "+fetchSize+" limit: "+limit+" split: "+splitRS);
		
		// add limit and determine to use scroll
		if(info.getAggregation() != null) {
			req = req.setSize(0);
		} else{
			if(limit > 0 && limit < fetchSize){ // no scroll needed
				req.setSize(limit);
			} else{ // use scrolling
				req.setSize(fetchSize);
				req.setScroll(new TimeValue(Utils.getIntProp(props, Utils.PROP_SCROLL_TIMEOUT_SEC, 60)*1000));
				if (info.getSorts().isEmpty()) req.addSort("_doc", SortOrder.ASC); // scroll works fast with sort on _doc
			}
		}
		
		// use query cache when this was indicated in FROM clause
		if(info.getUseCache()) req.setRequestCache(true);
		req.setTimeout(TimeValue.timeValueMillis(Utils.getIntProp(props, Utils.PROP_QUERY_TIMEOUT_MS, 10000)));
	}
	
	/**
	 * Builds the request defined within the explain statement and returns its string representation
	 * @param sql
	 * @param explain
	 * @param indexes
	 * @return
	 * @throws SQLException
	 */
	public String explain(String sql, Explain explain, String... indexes) throws SQLException {
		com.facebook.presto.sql.tree.Statement explanSt = explain.getStatement();
		if(!(explanSt instanceof Query)) throw new SQLException("Can only EXPLAIN SELECT ... statements");
		this.buildRequest(sql, ((Query)explanSt).getQueryBody(), indexes);
		return this.request.toString();
	}

	/**
	 * Executes the current query and returns the first ResultSet if query was successful
	 * @return
	 * @throws SQLException
	 */
	public ResultSet execute() throws SQLException {
		return this.execute(Utils.getBooleanProp(props, Utils.PROP_RESULT_NESTED_LATERAL, true));
	}
	
	/**
	 * Used by {@link ESUpdateState} to execute a query and force nested view. This result can be used to
	 * to insert data or delete rows. 
	 * @param useLateral
	 * @return
	 * @throws SQLException
	 */
	ResultSet execute(boolean useLateral) throws SQLException{
		if(request == null) throw new SQLException("Unable to execute query because it has not correctly been parsed");
		//System.out.println(request);
		this.esResponse = this.request.execute().actionGet();
		//System.out.println(esResponse);
		ESResultSet rs = convertResponse(useLateral);
		if(rs == null) throw new SQLException("No result found for this query");
		if(this.result != null) this.result.close();
		this.result = rs;
		return this.result;
	}

	/**
	 * Parses the result from ES and converts it into an ESResultSet object
	 * @return
	 * @throws SQLException
	 */
	private ESResultSet convertResponse(boolean useLateral) throws SQLException{
		if(esResponse.getHits().getHits().length == 0 && esResponse.getScrollId() != null){
			esResponse = client.prepareSearchScroll(esResponse.getScrollId())
					.setScroll(new TimeValue(Utils.getIntProp(props, Utils.PROP_SCROLL_TIMEOUT_SEC, 60)*1000))
					.execute().actionGet();
		}
		// parse aggregated result
		if(esResponse.getAggregations() != null){
			ESResultSet rs = new ESResultSet(this);
			for(Aggregation agg : esResponse.getAggregations()){
				aggParser.parseAggregation(agg, rs);
			}
			if(rs.getNrRows() == 0) return null;
			if(having != null) rs.filterHaving(having);
			rs.setTotal(rs.getNrRows());
			if(!orderings.isEmpty()){
				rs.orderBy(orderings);
			}
			if(this.limit > -1) rs.limit(limit);
			rs.executeComputations();
			return rs;
		}else{
			// parse plain document hits
			long total = esResponse.getHits().getTotalHits().value;
			if(limit > 0) total = Math.min(total, limit);
			ESResultSet rs = hitParser.parse(esResponse.getHits(), this.statement, this.heading, total, Utils.getIntProp(props, Utils.PROP_DEFAULT_ROW_LENGTH, 1000), useLateral, 0, null);
			
			while(rs.rowCount() < Math.min(maxRowsRS, rs.getTotal() - rs.getOffset())){
				// keep adding data to the resultset as long as there are more results available
				esResponse = client.prepareSearchScroll(esResponse.getScrollId())
						.setScroll(new TimeValue(Utils.getIntProp(props, Utils.PROP_SCROLL_TIMEOUT_SEC, 60)*1000))
						.execute().actionGet();
				rs = hitParser.parse(esResponse.getHits(), this.statement, this.heading, total, Utils.getIntProp(props, Utils.PROP_DEFAULT_ROW_LENGTH, 1000), useLateral, 0, rs);
				// make sure the resultset does not contain more results than requested 
				rs.setTotal(Math.min(esResponse.getHits().getTotalHits().value, limit>0 ? limit : esResponse.getHits().getTotalHits().value));
				
			}			
			
			rs.executeComputations();
			return rs;
		}
	}
	
	public ResultSet moreResults(boolean useLateral) throws SQLException {
		if(result != null && result.getOffset() + result.getNrRows() >= result.getTotal()) return null;
		if(result != null) result.close();
		if(esResponse.getScrollId() != null ){
			esResponse = client.prepareSearchScroll(esResponse.getScrollId())
					.setScroll(new TimeValue(Utils.getIntProp(props, Utils.PROP_SCROLL_TIMEOUT_SEC, 60)*1000))
					.execute().actionGet();
			ESResultSet rs = convertResponse(useLateral);
			rs.setOffset(result.getOffset() + result.getNrRows());
			if(rs.getNrRows() == 0) return null;
			result = rs;
			return result;
		}
		return null;
	}
	
	public Heading getHeading() {
		return heading;
	}
	
	public Statement getStatement(){
		return statement;
	}

	public void close() throws SQLException {
		if(this.esResponse != null && this.esResponse.getScrollId() != null){
			client.prepareClearScroll().addScrollId(this.esResponse.getScrollId()).execute();
		}
		if(this.result != null) result.close();
	}

	/**
	 * Allows to set a limit other than using LIMIT in the SQL
	 */
	public void setMaxRows(int size){
		if(size > 0) this.maxRowsRS = size;
		else this.maxRowsRS = Integer.MAX_VALUE;
		if(this.maxRowsRS < fetchSize) fetchSize = maxRowsRS;
	}
	
	public int getMaxRows(){
		return maxRowsRS;
	}
	
	public ESQueryState copy() throws SQLException{
		return new ESQueryState(client, statement);
	}
	
	public int getIntProp(String name, int def) {
		return Utils.getIntProp(props, name, def);
	}
	
}


