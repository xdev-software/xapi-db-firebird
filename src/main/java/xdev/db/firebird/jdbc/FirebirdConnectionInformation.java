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

import com.xdev.jadoth.sqlengine.dbms.DbmsConnectionInformation;

import xdev.db.ConnectionInformation;


public class FirebirdConnectionInformation extends ConnectionInformation<FirebirdDbms>
{
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	/**
	 * @param user         the user
	 * @param password     the password
	 * @param database     the database
	 * @param urlExtension the extended url properties
	 * @param dbmsAdaptor  the dbms adaptor
	 */
	public FirebirdConnectionInformation(
		final String host, final int port, final String user,
		final String password, final String database, final String urlExtension,
		final FirebirdDbms dbmsAdaptor)
	{
		super(host, port, user, password, database, urlExtension, dbmsAdaptor);
	}
	
	// /////////////////////////////////////////////////////////////////////////
	// getters //
	// ///////////////////
	
	/**
	 * Gets the database.
	 *
	 * @return the database
	 */
	public String getDatabase()
	{
		return this.getCatalog();
	}
	
	// /////////////////////////////////////////////////////////////////////////
	// setters //
	// ///////////////////
	
	/**
	 * Sets the database.
	 *
	 * @param database the database to set
	 */
	public void setDatabase(final String database)
	{
		this.setCatalog(database);
	}
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	
	/**
	 * @see DbmsConnectionInformation#createJdbcConnectionUrl()
	 */
	@Override
	public String createJdbcConnectionUrl()
	{
		final String url = "jdbc:firebirdsql:" + this.getHost() + "/" + this.getPort() + ":" + this.getCatalog();
		return this.appendUrlExtension(url);
	}
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsConnectionInformation#getJdbcDriverClassName()
	 */
	@Override
	public String getJdbcDriverClassName()
	{
		return "org.firebirdsql.jdbc.FBDriver";
	}
}
