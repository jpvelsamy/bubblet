package in.pazmysaz.essql.model;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import in.pazmysaz.essql.QueryState;
import in.pazmysaz.essql.model.Column.Operation;

/**
 * An implementation of {@link QueryState} used to hold information created during SQL parsing by the Presto parser.
 * Most importantly it hods the {@link Heading} containing the fields to be requested which is used throughout the 
 * query execution and parsing of results.
 * 
 * @author cversloot
 *
 */
public class BasicQueryState implements QueryState {

	private Heading heading;
	private Properties props;
	private String sql;
	private SQLException exception = null;
	private HashMap<String, Object> kvStore = new HashMap<String, Object>();
	private List<QuerySource> relations = new ArrayList<QuerySource>();
	private Map<String, Map<String, Integer>> esInfo;

	public BasicQueryState(String sql, Heading heading, Properties props){
		this.heading = heading;
		this.props = props;
		this.sql = sql;
	}
	
	@Override
	public String originalSql() {
		return sql;
	}

	@Override
	public Heading getHeading() {
		return heading;
	}

	@Override
	public List<QuerySource> getSources(){
		return this.relations ;
	}
	
	public void setRelations(List<QuerySource> relations){
		this.relations = relations;
	}
	
	@Override
	public void addException(String msg) {
		this.exception = new SQLException(msg);		
	}

	@Override
	public boolean hasException() {
		return this.exception != null;
	}

	@Override
	public SQLException getException() {
		return this.exception;
	}

	@Override
	public int getIntProp(String name, int def) {
		return Utils.getIntProp(props, name, def);
	}

	@Override
	public String getProperty(String name, String def) {
		if(!this.props.containsKey(name)) return def;
		try {
			return this.props.getProperty(name);
		} catch (Exception e) {
			return def;
		}
	}

	@Override
	public Object getProperty(String name) {
		return this.props.get(name);
	}

	@Override
	public QueryState setKeyValue(String key, Object value) {
		kvStore.put(key, value);
		return this;
	}

	@Override
	public Object getValue(String key) {
		return kvStore.get(key);
	}

	@Override
	public String getStringValue(String key) {
		return (String)kvStore.get(key);
	}

	@Override
	public boolean isCountDistinct() {
		for(Column col : getHeading().columns()){
			if(col.getOp() == Operation.COUNT_DISTINCT) return true;
		}
		return false;
	}
	
	public Map<String, Map<String, Integer>> getEsInfo() {
		esInfo = (Map<String, Map<String, Integer>>)Utils.getObjectProperty(props, Utils.PROP_TABLE_COLUMN_MAP);
		return esInfo;
	}

}
