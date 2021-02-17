package in.pazmysaz.essql.model.expression;

import java.sql.SQLException;
import java.util.List;

public interface IComparison {

	public boolean evaluate(List<Object> row) throws SQLException;
	
}
