package in.pazmysaz.essql.parse.se;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import in.pazmysaz.essql.ESArray;
import in.pazmysaz.essql.ESResultSet;
import in.pazmysaz.essql.model.Column;
import in.pazmysaz.essql.model.Heading;
import in.pazmysaz.essql.model.Utils;
import in.pazmysaz.essql.model.Column.Operation;

/**
 * Responsible to parse {@link SearchHits} from an elasticsearch result into an {@link ESResultSet}. 
 * Nested objects are exploded which means that a single document can be parsed into into one or more rows.
 * A document that contains for example a list containing three objects will result in three rows in the
 * final resultset.
 * 
 * Which fields are read from the document depends on the fields specified in the SQL query. Selecting everything
 * using a '*' will get all fields. If the root of a nested object is specified it will parse all fields of
 * that object. For example 'SELECT nestedDoc FROM type' will result in columns: nestedDoc.field1, nestedDoc.field2 etc.
 * 
 * @author cversloot
 *
 */
public class SearchHitParser {
	
	/**
	 * Parses the SearchHits portion of an elasticsearch search result.
	 * @param hits
	 * @param head
	 * @throws SQLException 
	 */
	public ESResultSet parse(SearchHits hits, Statement statement, Heading head, long total, int rowLength, boolean useLateral, long offset, ESResultSet previous) throws SQLException{
		Map<String, Heading> headMap = buildHeaders(head);
		
		/*headMap.forEach((k,v)->{
			System.out.println("[Final Head Map] Key = "+k);
			System.out.println("[Final Head Map]Value="+v);
		});*/
		
		ESResultSet rs;
		if(previous != null){
			rs = previous;
		}else{
			rs = new ESResultSet(statement, head, (int)total, rowLength);
			rs.setOffset((int)offset);
		}
		for(SearchHit hit : hits){
			this.parse(hit.getSourceAsMap(), statement, hit, rs, useLateral, "", headMap);
		}
		
		if(useLateral){
			// remove any temporary columns used during parsing of the results 
			ArrayList<Column> tmpCols = new ArrayList<Column>();
			for(Column col : rs.getHeading().columns()){
				if(rs.getHeading().hasLabelStartingWith(col.getColumn()+".")) {
					tmpCols.add(col);
					//col.setVisible(false);
				}
			}
			for(Column col : tmpCols) col.setVisible(false);//rs.getHeading().remove(col); 
		}else{
			for(Column col : rs.getHeading().columns())
				if(col.getColumn().contains(".") ) col.setVisible(false);
		}
		return rs;
	}
	
	/**
	 * Builds all the headers (for all the resultsets, top and nested) needed during parsing of the result.
	 * This index is used to determine if a field from a result must be added to a resultset or not
	 * @param heading
	 * @return
	 */
	public Map<String, Heading> buildHeaders(Heading heading) {
		//System.out.println("[buildHeaders] 1. Inbound Heading name=" + heading);
		Map<String, Heading> headMap = new HashMap<String, Heading>();
		for (int i = 0; i < 100; i++) {
			
			col: for (Column col : heading.columns()) {
				String column = col.getColumn();
				
				String[] parentAndKey = parentKey(column, i);
				if (parentAndKey == null)
					continue;
				String key = parentAndKey[0];
				String label = parentAndKey[1];
				
				Heading newH = headMap.get(key);
			
				if (newH == null) 
				{
					newH = new Heading();
					headMap.put(key, newH);
					for (Map.Entry<String, Heading> entry : headMap.entrySet()) 
					{
						String key2 = entry.getKey();
						Heading value2 = entry.getValue();
						if (key.startsWith(key2) && value2.hasAllCols()) 
						{
							//System.out.println("[buildHeaders] BAILING OUT!, key="+key+", starts with ="+key2+", value2 has all cols="+value2.hasAllCols());
							continue col;
						}
					}
					
				}
				
				
				if (label == null) 
				{
					newH = new Heading().setAllColls(true);
					headMap.put(key, newH);
				} else if (!newH.hasLabel(label) && !newH.hasAllCols()) 
				{
					newH.add(new Column(label));					
				}
			}
		System.out.println("[buildHeaders] headMap=" + headMap+", on counter ="+i);
		}
		if (!headMap.containsKey(""))
			headMap.put("", new Heading().setAllColls(true));
		if (heading.hasAllCols())
			headMap.get("").setAllColls(true);
		return headMap;
	}
	
	
	
	/**
	 * Splits a name formatted like a.b.c into a parent 'a.b' and key 'c' for a given index (2 in this example)
	 * @param name
	 * @param idx
	 * @return
	 */
	private String[] parentKey(String name, int idx) {
		String[] parts = name.split("\\.");
		if(idx > parts.length) return null;
		String parent = "";
		String key = null;
		if(idx < parts.length) key = parts[idx];
		for(int i=0; i<idx; i++) {
			parent = parent.length() > 0 ? parent+"."+parts[i] : parts[i]; 
		}
		return new String[]{parent, key};
	}

	/**
	 * Parses the provided source and puts the elements in a row within the {@link ESResultSet}
	 * @param source
	 * @param rs
	 * @throws SQLException 
	 */
	@SuppressWarnings("unchecked")
	private void parse(Map<String, ?> source, Statement statement, SearchHit hit, ESResultSet rs, boolean explode, String parent, Map<String, Heading> headMap) throws SQLException{
		Heading head = rs.getHeading();
		List<Object> row = rs.getNewRow();
		if(hit != null)
			addIdIndexAndType(hit.getId(), hit.getIndex(), hit.getType(), hit.getScore(), null/*hit.getHighlightFields()*/ , head, row);
		if(source == null) 
			return; // just return if source was not stored!
		for(Map.Entry<String, ?> entry : source.entrySet())
		{
			String key = entry.getKey();
			String fullKey = parent.length()>0 ? parent+"."+ key : key;

			boolean containsFullKey = headMap.containsKey(fullKey);
			boolean hasAllCols = head.hasAllCols();
			//System.out.println("[parse] fullKey="+fullKey+", contains full key="+containsFullKey+", hasAllCols="+hasAllCols);
			if(!containsFullKey && !hasAllCols) {
				System.err.println("Something is wrong, [parse] fullKey="+fullKey+" is absent in metadata");
				AtomicBoolean deepPresent = new AtomicBoolean(false);
				headMap.forEach((columnName, columnHeading)->{
					//System.out.println("[parse] manually deep checking column="+columnName+", for fullkey="+fullKey);
					if(columnName.trim().toLowerCase().equals(fullKey.trim().toLowerCase())) {
						//System.out.println("[parse] fullKey="+fullKey+" is present with lower case and trim deep search");
						deepPresent.set(true);
					}
				});
				if(!deepPresent.get())
				continue;
			}
			
			if( entry.getValue() instanceof Map )
			{
				parseMapIntoResultSet(key, statement, head, row, explode, fullKey, headMap, (Map<String, ?>)entry.getValue());
			}else if(entry.getValue() instanceof List) 
			{
				List<Object> list = (List<Object>)entry.getValue();
				if(list.size() > 0)
				{
					if(list.get(0) instanceof Map)
					{
						parseMapIntoResultSet(key, statement, head, row, explode, fullKey, headMap, list.toArray());
					}else
					{
						if(head.hasLabel(key))
						{
							Column s = head.getColumnByLabel(key);
							s.setSqlType(Types.ARRAY);
							row.set(s.getIndex(), new ESArray(list));
						}else if(hasAllCols)
						{
							Column newCol = new Column(key).setSqlType(Types.ARRAY);
							head.add(newCol);
							row.set(newCol.getIndex(), new ESArray(list));
						}
					}
				}else
				{
					// add column if it does not exist yet and set value to null
					if(hasAllCols && !head.hasLabel(key))
					{
						head.add(new Column(key));
					}
				}
			}else
			{
				addValueToRow(key, entry.getValue(), head, row);
			}
		}
		if(explode){
			List<List<Object>> rows = explodeRow(head, row);
			for(List<Object> exRow : rows) rs.add(exRow);
		}else{
			rs.add(row);
		}
	}
	

	/**
	 * Explodes any nested objects within the provided row. This produces multiple rows, each
	 * with a different combination of nested information.
	 * @param row
	 * @param head
	 * @return
	 * @throws SQLException 
	 */
	private List<List<Object>> explodeRow(Heading head, List<Object> row) throws SQLException {
		List<List<Object>> rows = new ArrayList<List<Object>>();
		rows.add(row);
		for(int ri=0; ri<head.getColumnCount(); ri++){
			if(!head.getColumn(ri).isVisible()) continue;

			Object element = row.get(ri);
			if(element instanceof ESResultSet){
				ESResultSet nested = (ESResultSet)element;
				String parent = head.getColumn(ri).getColumn();
				
				long nestedCount = nested.getTotal(); 
				if(nestedCount == 0) {
					nested.close();
					continue;
				}
				
				// add rows to hold the nested data
				int rowCount = rows.size();
				for(int n=0; n<nestedCount-1; n++) 
					for(int i=0; i<rowCount; i++) rows.add(Utils.clone(rows.get(i)));
				
				// now add a nested row to each of the rows in the final resultset
				for(int i=0; i<rows.size(); i++){
					List<Object> destinationRow = rows.get(i);
					List<Object> nestedRow = nested.getRow(i%(int)nestedCount);
					for(Column nestedCol : nested.getHeading().columns()){
						String nestedColName = parent+"."+nestedCol.getColumn();
						
						Column destinationCol = head.getColumnByLabel(nestedColName);
						if(destinationCol == null){
							destinationCol = new Column(nestedColName).setAlias(nestedCol.getAlias())
									.setSqlType(nestedCol.getSqlType()).setVisible(nestedCol.isVisible());
							head.add(destinationCol);
						}
						Object value = nestedRow.get(nestedCol.getIndex());
						destinationRow.set(destinationCol.getIndex(), value);
					}
				}
				nested.close();
				row.set(ri, null);
			}
		}
		
		return rows;
	}

	/**
	 * Adds _id, _index and/or _type to the current row
	// * @param idIndexTypeScore = String[]{_id, _index, _type}
	 * @param head
	 * @param row
	 */
	private void addIdIndexAndType(String id, String index, String type, Float score, Map<String, HighlightBuilder.Field> highlights, Heading head, List<Object> row){
		if(id != null && head.hasAllCols() || head.hasLabel(Heading.ID)){
			row.set( head.getColumnByLabel(Heading.ID).getIndex(), id);
		}
		if(index != null && head.hasAllCols() || head.hasLabel(Heading.INDEX)){
			row.set( head.getColumnByLabel(Heading.INDEX).getIndex(), index);
		}
		if(type != null && head.hasAllCols() || head.hasLabel(Heading.TYPE)){
			row.set( head.getColumnByLabel(Heading.TYPE).getIndex(), type);
		}
		if(score != null && head.hasLabel(Heading.SCORE)){
			row.set( head.getColumnByLabel(Heading.SCORE).getIndex(), score);
		}
/*		if(highlights != null){
			for(String field : highlights.keySet()){
				Column col = head.getColumnByNameAndOp(field, Operation.HIGHLIGHT);
				if(col == null) continue;
				List<Object> fragments = new ArrayList<Object>();
				for(Text fragment : highlights.get(field).getFragments()) fragments.add(fragment.toString());
				row.set(col.getIndex(), new ESArray(fragments));
			}
		}*/
	}
	
	/**
	 * Adds a single value to its correct place in the row 
	 * @param key
	 * @param value
	 * @param heading
	 * @param row
	 */
	private void addValueToRow(String key, Object value, Heading heading, List<Object> row){
		if(heading.hasLabel(key)){
			Column col = heading.getColumnByLabel(key);
/*			if (col.getSqlType() == Types.TIMESTAMP) {

			} else {*/
				row.set(col.getIndex(), value);
/*			}*/
		}else if(heading.hasAllCols()){
			int type = Heading.getTypeIdForObject(value);
			Column newCol = new Column(key).setSqlType(type);
			heading.add(newCol);
			row.set(newCol.getIndex(), value);
		}
	}
	
	/**
	 * Parses the provided set of nested Objects (map<String, ?> ) into an ResultSet which is 
	 * added to the row.
	 * @param key
	 //* @param value
	 * @param heading
	 * @param row
	 * @throws SQLException 
	 */
	@SuppressWarnings("unchecked")
	private void parseMapIntoResultSet(String key, Statement statement, Heading heading, List<Object> row, boolean explode, String parent, Map<String, Heading> headMap, Object... nestedObjects) throws SQLException{
		Heading nestedHeading = headMap.get(parent);
		if(nestedHeading == null) {
			nestedHeading = new Heading().setAllColls(true);
			headMap.put(parent, nestedHeading);
		}
		ESResultSet nestedRs = new ESResultSet(statement, nestedHeading, nestedObjects.length, 1000);
		for(Object object : nestedObjects){
			parse((Map<String, ?>)object, statement, null, nestedRs, explode, parent, headMap);
		}
		Column col = heading.getColumnByLabel(key);
		if(col == null){
			col = new Column(key).setSqlType(Types.JAVA_OBJECT);
			heading.add(col);
		}
		row.set(col.getIndex(), nestedRs);
	}
}
