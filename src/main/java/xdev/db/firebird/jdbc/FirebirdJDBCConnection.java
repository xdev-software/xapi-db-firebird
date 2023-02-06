package xdev.db.firebird.jdbc;

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


import java.io.Reader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import xdev.db.DBException;
import xdev.db.Result;
import xdev.db.jdbc.JDBCConnection;


public class FirebirdJDBCConnection extends JDBCConnection<FirebirdJDBCDataSource, FirebirdDbms>
{
	public FirebirdJDBCConnection(FirebirdJDBCDataSource dataSource)
	{
		super(dataSource);
	}
	
	
	@Override
	public int getQueryRowCount(String select) throws DBException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT COUNT(*) FROM (");
		sb.append(select);
		sb.append(")");
		
		Result result = query(sb.toString());
		try
		{
			result.next();
			int rowCount = result.getInt(0);
			return rowCount;
		}
		finally
		{
			result.close();
		}
	}
	
	
	@Override
	protected void prepareParams(Connection connection, Object[] params) throws DBException
	{
		super.prepareParams(connection,params);
		
		if(params == null)
		{
			return;
		}
		
		for(int i = 0; i < params.length; i++)
		{
			Object param = params[i];
			if(param == null)
			{
				continue;
			}
			
			if(param instanceof Clob)
			{
				try
				{
					Reader characterStream = ((Clob)param).getCharacterStream();
					String paramString = "";
					for(int j; (j = characterStream.read()) > 0;)
					{
						paramString = paramString + (char)j;
					}
					
					params[i] = paramString;
				}
				catch(Exception e)
				{
					throw new DBException(getDataSource(),"Parameter + " + i
							+ " from Type java.sql.Clob cannot parsed to String");
				}
			}
			
		}
		
	}
	
	
	@Override
	public void createTable(String tableName, String primaryKey, Map<String, String> columnMap,
			boolean isAutoIncrement, Map<String, String> foreignKeys) throws Exception
	{
		Connection connection = super.getConnection();
		Statement statement = connection.createStatement();
		try
		{
			if(!checkIfTableExists(connection.createStatement(),tableName))
			{
				
				if(!columnMap.containsKey(primaryKey))
				{
					columnMap.put(primaryKey,"INTEGER"); //$NON-NLS-1$
				}
				StringBuffer createStatement = null;
				
				createStatement = new StringBuffer("CREATE TABLE " + tableName + "(" //$NON-NLS-1$ //$NON-NLS-2$
						+ primaryKey + " " + columnMap.get(primaryKey) + " NOT NULL,"); //$NON-NLS-1$ //$NON-NLS-2$
				
				for(String keySet : columnMap.keySet())
				{
					if(!keySet.equals(primaryKey))
					{
						createStatement.append(keySet + " " + columnMap.get(keySet) + ","); //$NON-NLS-1$ //$NON-NLS-2$
					}
				}
				
				createStatement.append(" PRIMARY KEY (" + primaryKey + "))"); //$NON-NLS-1$ //$NON-NLS-2$
				
				if(log.isDebugEnabled())
				{
					log.debug("SQL Statement to create a table: " + createStatement.toString()); //$NON-NLS-1$
				}
				statement.execute(createStatement.toString());
			}
		}
		catch(Exception e)
		{
			throw e;
		}
		finally
		{
			statement.close();
			connection.close();
		}
	}
	
	
	private boolean checkIfTableExists(Statement statement, String tableName) throws Exception
	{
		
		String sql = "SELECT * FROM rdb$relations where rdb$relation_name = '" + tableName + "'"; //$NON-NLS-1$ //$NON-NLS-2$
		
		if(log.isDebugEnabled())
		{
			log.debug(sql);
		}
		
		ResultSet resultSet = null;
		try
		{
			statement.execute(sql);
			resultSet = statement.getResultSet();
		}
		catch(Exception e)
		{
			if(resultSet != null)
			{
				resultSet.close();
			}
			statement.close();
			throw e;
		}
		
		if(resultSet != null)
		{
			while(resultSet.next())
			{
				resultSet.close();
				statement.close();
				return true;
				
			}
			resultSet.close();
			
		}
		statement.close();
		return false;
	}
	
	
	@Override
	public Date getServerTime() throws DBException, ParseException
	{
		String selectTime = "SELECT current_timestamp from rdb$database"; //$NON-NLS-1$
		return super.getServerTime(selectTime);
	}
	
}
