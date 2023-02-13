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

import xdev.db.DBException;
import xdev.db.jdbc.JDBCDataSource;


public class FirebirdJDBCDataSource extends JDBCDataSource<FirebirdJDBCDataSource, FirebirdDbms>
{
	public FirebirdJDBCDataSource()
	{
		super(new FirebirdDbms());
	}
	
	@Override
	public Parameter[] getDefaultParameters()
	{
		return new Parameter[]{
			HOST.clone(), PORT.clone(3050), USERNAME.clone("sysdba"),
			PASSWORD.clone(), CATALOG.clone(), URL_EXTENSION.clone(),
			IS_SERVER_DATASOURCE.clone(), SERVER_URL.clone(), AUTH_KEY.clone()};
	}
	
	@Override
	protected FirebirdConnectionInformation getConnectionInformation()
	{
		return new FirebirdConnectionInformation(getHost(), getPort(), getUserName(), getPassword()
			.getPlainText(), getCatalog(), getUrlExtension(), getDbmsAdaptor());
	}
	
	@Override
	public FirebirdJDBCConnection openConnectionImpl() throws DBException
	{
		return new FirebirdJDBCConnection(this);
	}
	
	@Override
	public FirebirdJDBCMetaData getMetaData() throws DBException
	{
		return new FirebirdJDBCMetaData(this);
	}
	
	@Override
	public boolean canExport()
	{
		return false;
	}
}
