package in.pazmysaz.essql.model.expression;

import com.facebook.presto.sql.tree.ArithmeticUnaryExpression.Sign;

import in.pazmysaz.essql.ESResultSet;

public interface ICalculation {

	public Number evaluate(ESResultSet result, int rowNr);
	
	public ICalculation setSign(Sign sign);
	
}
