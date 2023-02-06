/*
 * SqlEngine Database Adapter Firebird - XAPI SqlEngine Database Adapter for Firebird
 * Copyright © 2003 XDEV Software (https://xdev.software)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;

import xdev.db.ColumnMetaData;
import xdev.db.DBException;
import xdev.db.DataType;
import xdev.db.Index;
import xdev.db.StoredProcedure;
import xdev.db.StoredProcedure.Param;
import xdev.db.StoredProcedure.ParamType;
import xdev.db.StoredProcedure.ReturnTypeFlavor;
import xdev.db.jdbc.JDBCColumnsMetaData;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCMetaData;
import xdev.util.ProgressMonitor;


public class FirebirdJDBCMetaData extends JDBCMetaData
{
	
	private static final String	PROCEDURE_SCHEM	= "PROCEDURE_SCHEM";
	private static final String	PROCEDURE_NAME	= "PROCEDURE_NAME";
	private static final String	PROCEDURE_TYPE	= "PROCEDURE_TYPE";
	private static final String	SYS				= "SYS";
	private static final String	SQLJ			= "SQLJ";
	private static final String	COLUMN_NAME		= "COLUMN_NAME";
	private static final String	COLUMN_TYPE		= "COLUMN_TYPE";
	private static final String	DATA_TYPE		= "DATA_TYPE";
	private static final String	REMARKS			= "REMARKS";
	
	
	/**
	 * @since 4.0 if Procedure_Type value=2, ReturnType is set to void. It is
	 *        not as designed!
	 */
	@Override
	public StoredProcedure[] getStoredProcedures(ProgressMonitor monitor) throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		
		List<StoredProcedure> list = new ArrayList<>();
		
		try
		{
			ConnectionProvider connectionProvider = dataSource.getConnectionProvider();
			Connection connection = connectionProvider.getConnection();
			
			try
			{
				DatabaseMetaData meta = connection.getMetaData();
				
				String catalog = getCatalog(dataSource);
				String schema = getSchema(dataSource);
				
				// Stored Procedures
				ResultSet procedures = meta.getProcedures(catalog,schema,null);
				ResultSet procedureColumns = meta.getProcedureColumns(catalog,schema,null,null);
				
				Map<String, List<JDBCColumnsMetaData>> procedureColumnsMap = columnsResultSetToMap(procedureColumns);
				addStoredProcedures(list,procedures,procedureColumnsMap);
				procedures.close();
				procedureColumns.close();
			}
			finally
			{
				connection.close();
			}
		}
		catch(SQLException e)
		{
			throw new DBException(dataSource,e);
		}
		
		monitor.done();
		
		return list.toArray(new StoredProcedure[list.size()]);
	}
	
	
	private Map<String, List<JDBCColumnsMetaData>> columnsResultSetToMap(ResultSet resultSet)
			throws SQLException
	{
		Map<String, List<JDBCColumnsMetaData>> resultMap = new HashMap<String, List<JDBCColumnsMetaData>>();
		while(resultSet.next())
		{
			
			String name = resultSet.getString(PROCEDURE_NAME);
			
			// skip system functions
			String schema = resultSet.getString(PROCEDURE_SCHEM);
			if(schema != null && (schema.startsWith(SYS) || schema.startsWith(SQLJ)))
			{
				continue;
			}
			
			String columnName = resultSet.getString(COLUMN_NAME);
			int columnType = resultSet.getInt(COLUMN_TYPE);
			
			DataType dataType = DataType.get(resultSet.getInt(DATA_TYPE));
			
			if(resultMap.containsKey(name))
			{
				List<JDBCColumnsMetaData> metaDataList = resultMap.get(name);
				JDBCColumnsMetaData metaData = new JDBCColumnsMetaData(dataType,columnType,
						columnName);
				
				metaDataList.add(metaData);
				
			}
			else
			{
				List<JDBCColumnsMetaData> metaDataList = new ArrayList<JDBCColumnsMetaData>();
				JDBCColumnsMetaData metaData = new JDBCColumnsMetaData(dataType,columnType,
						columnName);
				metaDataList.add(metaData);
				
				resultMap.put(name,metaDataList);
			}
			
		}
		resultSet.close();
		return resultMap;
	}
	
	
	private void addStoredProcedures(List<StoredProcedure> list, ResultSet resultSet,
			Map<String, List<JDBCColumnsMetaData>> resultMap) throws SQLException
	{
		while(resultSet.next())
		{
			
			// skip system functions
			String schema = resultSet.getString(PROCEDURE_SCHEM);
			if(schema != null && (schema.startsWith(SYS) || schema.startsWith(SQLJ)))
			{
				continue;
			}
			
			String name = resultSet.getString(PROCEDURE_NAME);
			String description = resultSet.getString(REMARKS);
			ReturnTypeFlavor returnTypeFlavor = null;
			DataType returnType = null;
			
			switch(resultSet.getInt(PROCEDURE_TYPE))
			{
				case DatabaseMetaData.procedureNoResult:
					// XXX PROCEDURE_TYPE value is 2 for void, but normally it
					// should be value=1. Check this behavior in future firebird
					// versions.
				case 2:
					returnTypeFlavor = ReturnTypeFlavor.VOID;
				break;
				
				default:
					returnTypeFlavor = ReturnTypeFlavor.UNKNOWN;
			}
			
			// search for a procedure column
			if(resultMap.containsKey(name))
			{
				addStoredProceduresWithParams(list,resultMap,name,description,returnTypeFlavor,
						returnType);
			}
			else
			{
				// add without params
				list.add(new StoredProcedure(returnTypeFlavor,returnType,name,description,
						new Param[0]));
			}
			
		}
		resultSet.close();
	}
	
	
	private void addStoredProceduresWithParams(List<StoredProcedure> list,
			Map<String, List<JDBCColumnsMetaData>> resultMap, String procName, String description,
			ReturnTypeFlavor returnTypeFlavor, DataType returnType)
	{
		
		if(procName != null)
		{
			
			List<JDBCColumnsMetaData> metaDataList = resultMap.get(procName);
			List<Param> params = new ArrayList<Param>();
			for(JDBCColumnsMetaData jdbcColumnsMetaData : metaDataList)
			{
				
				if(jdbcColumnsMetaData.getColumnName() != null)
				{
					
					switch(jdbcColumnsMetaData.getColumnType())
					{
						case DatabaseMetaData.procedureColumnReturn:
							returnTypeFlavor = ReturnTypeFlavor.TYPE;
							returnType = jdbcColumnsMetaData.getDataType();
						break;
						
						case DatabaseMetaData.procedureColumnResult:
							returnTypeFlavor = ReturnTypeFlavor.RESULT_SET;
						break;
						
						case DatabaseMetaData.procedureColumnIn:
							params.add(new Param(ParamType.IN,jdbcColumnsMetaData.getColumnName(),
									jdbcColumnsMetaData.getDataType()));
						break;
						
						case DatabaseMetaData.procedureColumnOut:
							params.add(new Param(ParamType.OUT,jdbcColumnsMetaData.getColumnName(),
									jdbcColumnsMetaData.getDataType()));
						break;
						
						case DatabaseMetaData.procedureColumnInOut:
							params.add(new Param(ParamType.IN_OUT,jdbcColumnsMetaData
									.getColumnName(),jdbcColumnsMetaData.getDataType()));
						break;
					}
					
				}
				else
				{
					returnType = jdbcColumnsMetaData.getDataType();
				}
				
			}
			list.add(new StoredProcedure(returnTypeFlavor,returnType,procName,description,params
					.toArray(new Param[params.size()])));
		}
		
	}
	
	
	public FirebirdJDBCMetaData(FirebirdJDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	
	@Override
	protected void createTable(JDBCConnection jdbcConnection, TableMetaData table)
			throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void addColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData columnBefore, ColumnMetaData columnAfter)
			throws DBException, SQLException
	{
		;
	}
	
	
	@Override
	protected void alterColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData existing) throws DBException, SQLException
	{
	}
	
	
	@Override
	public boolean equalsType(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		return false;
	}
	
	
	@Override
	protected void dropColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column) throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void createIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void dropIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
	}
}
