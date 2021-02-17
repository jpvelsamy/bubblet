package in.pazmysaz.essql.parse.sql;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.StringLiteral;

import in.pazmysaz.essql.QueryState;
import in.pazmysaz.essql.model.Column;
import in.pazmysaz.essql.model.Heading;
import in.pazmysaz.essql.model.expression.BooleanComparison;
import in.pazmysaz.essql.model.expression.IComparison;
import in.pazmysaz.essql.model.expression.SimpleComparison;

/**
 * A Presto {@link AstVisitor} implementation that parses GROUP BY clauses
 * 
 * @author cversloot
 *
 */
public class HavingParser extends AstVisitor<IComparison, QueryState>{
	
	@Override
	protected IComparison visitExpression(Expression node, QueryState state) {
		if( node instanceof LogicalBinaryExpression){
			LogicalBinaryExpression boolExp = (LogicalBinaryExpression)node;
			IComparison left = process(boolExp.getLeft(), state);
			IComparison right = process(boolExp.getRight(), state);
			return new BooleanComparison(left, right, boolExp.getType() == LogicalBinaryExpression.Type.AND);
		}else if( node instanceof ComparisonExpression){
			ComparisonExpression compareExp = (ComparisonExpression)node;
			Column column = new SelectParser().visitExpression(compareExp.getLeft(), state);
			Column leftCol = state.getHeading().getColumnByLabel(column.getLabel());
			if(leftCol == null){
				state.addException("Having reference "+column+" not found in SELECT clause");
				return null;
			}
			// right hand side is a concrete literal to compare with 
			if(compareExp.getRight() instanceof Literal){
				Object value;
				if(compareExp.getRight() instanceof LongLiteral) value = ((LongLiteral)compareExp.getRight()).getValue();
				else if(compareExp.getRight() instanceof BooleanLiteral) value = ((BooleanLiteral)compareExp.getRight()).getValue();
				else if(compareExp.getRight() instanceof DoubleLiteral) value = ((DoubleLiteral)compareExp.getRight()).getValue();
				else if(compareExp.getRight() instanceof StringLiteral) value = ((StringLiteral)compareExp.getRight()).getValue();
				else {
					state.addException("Unable to get value from "+compareExp.getRight());
					return null;
				}
				return new SimpleComparison(leftCol, compareExp.getType(), (Number)value);
			
				// right hand side refers to another column 	
			} else if(compareExp.getRight() instanceof DereferenceExpression || compareExp.getRight() instanceof Identifier){
				String col2;
				if(compareExp.getLeft() instanceof DereferenceExpression){
					// parse columns like 'reference.field'
					col2 = SelectParser.visitDereferenceExpression((DereferenceExpression)compareExp.getRight());
				}else{
					col2 = ((Identifier)compareExp.getRight()).getName(); //.getValue();
				}
				col2 = Heading.findOriginal(state.originalSql(), col2, "having.+", "\\W");
				Column rightCol = state.getHeading().getColumnByLabel(col2);
				if(rightCol == null){
					state.addException("column "+col2+" not found in SELECT clause");
					return null;
				}
				return new SimpleComparison(leftCol, compareExp.getType(), rightCol);
			}else { // unknown right hand side so
				state.addException("Unable to get value from "+compareExp.getRight());
				return null;
			}
			
		}else if( node instanceof NotExpression){
			state.addException("NOT is currently not supported, use '<>' instead");
		}else{
			state.addException("Unable to parse "+node+" ("+node.getClass().getName()+") is not a supported expression");
		}
		return null;
	}
	
}
