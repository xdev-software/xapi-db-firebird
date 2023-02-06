package xdev.db.firebird.jdbc;

import com.xdev.jadoth.sqlengine.SELECT;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler;


/*-
 * #%L
 * Firebird
 * %%
 * Copyright (C) 2003 - 2023 XDEV Software
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-3.0.html>.
 * #L%
 */
public class FirebirdDMLAssembler extends StandardDMLAssembler<FirebirdDbms>
{
	public FirebirdDMLAssembler(final FirebirdDbms dbms)
	{
		super(dbms);
	}
	
	
	@Override
	protected StringBuilder assembleSelectRowLimit(
		SELECT query, StringBuilder sb, int flags,
			String clauseSeperator, String newLine, int indentLevel)
	{
		Integer skip = query.getOffsetSkipCount();
		Integer range = query.getFetchFirstRowCount();
		
		if(range != null)
		{
			if(skip != null)
			{
				sb.append(newLine).append(clauseSeperator).append("ROWS ").append(skip)
						.append(" TO ").append(skip + range - 1);
			}
			else
			{
				sb.append(newLine).append(clauseSeperator).append("ROWS 1 TO ").append(range);
			}
		}
		else if(skip != null)
		{
			sb.append(newLine).append(clauseSeperator).append("ROWS ").append(skip).append(" TO ")
					.append(Integer.MAX_VALUE);
		}
		
		return sb;
	}
}
