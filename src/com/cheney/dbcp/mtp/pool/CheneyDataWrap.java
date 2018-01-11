package com.cheney.dbcp.mtp.pool;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import com.cheney.dbcp.mtp.utils.CheneyUtils;

@SuppressWarnings("unchecked")
public class CheneyDataWrap {
	
	private String[] strSQL;
	private Object[] simpleParams;
	
	private Boolean isSuccess;
	private Object simpleModel;
	private List<Object> complexModel;
	private String linear;
	private String[] simple;
	private String[][] complex;

	public CheneyDataWrap(Object[] simpleParams, String... strSQL) {
		this.simpleParams = simpleParams;
		this.strSQL = strSQL;
	}

	public CheneyDataWrap handler(Dimension dimension, Class<?> cls) {
		Connection con = CheneyConnectionPool.getConnection();
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Statement stmt = null;
		CallableStatement cstmt = null;
		if(dimension == Dimension.SIMPLE_BATCH || dimension == Dimension.COMPLEX_BATCH)
		{
			try {
				con.setAutoCommit(false);
				if (dimension == Dimension.SIMPLE_BATCH)
				{
					pstmt = con.prepareStatement(strSQL[0]);
					int count = CheneyUtils.getCharCount('?', strSQL[0]);
					int paramsCount = simpleParams.length / count;
					for (int i = 0; i < paramsCount; i++) {
						for (int j = 0; j < count; j++)
							CheneyUtils.handleParams(pstmt, j, simpleParams[i * count + j]);
						pstmt.addBatch();
					}
					pstmt.executeBatch();
					con.commit();
					pstmt.clearBatch();
				} else
				{
					stmt = con.createStatement();
					for (int i = 0; i < strSQL.length; i++)
						stmt.addBatch(strSQL[i]);
					stmt.executeBatch();
					con.commit();
					stmt.clearBatch();
				}
				con.setAutoCommit(true);
				isSuccess = Boolean.TRUE;
				return this;
			} catch (SQLException e) {
				CheneyUtils.exception(e);
				isSuccess = Boolean.FALSE;
				try {
					con.rollback();
					con.setAutoCommit(true);
					return this;
				} catch (SQLException ex) {
					CheneyUtils.exception(ex);
					isSuccess = Boolean.FALSE;
					return this;
				}
			}
		}
		else
		{
			try {
				if (dimension == Dimension.CALL)
					cstmt = con.prepareCall(strSQL[0], ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				else
					pstmt = con.prepareStatement(strSQL[0], ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				if (simpleParams != null)
					CheneyUtils.handleParams(pstmt == null ? cstmt : pstmt, simpleParams);
				if (dimension == Dimension.DML)
					isSuccess = !pstmt.execute();
				else
				{
					rs = pstmt == null ? cstmt.executeQuery() : pstmt.executeQuery();
					ResultSetMetaData rsmd = rs.getMetaData();
					int coulumnCount = rsmd.getColumnCount();
					StringBuilder builder = new StringBuilder();
					for(int i = 1;i <= coulumnCount; i++)
						builder.append(rsmd.getColumnLabel(i)).append(",");
					String alias = builder.deleteCharAt(builder.length() - 1).toString();
					if (dimension == Dimension.LINEAR)
					{
						rs.next();
						linear = rs.getObject(alias).toString();
					}
					else
					{
						rs.last();
						int row = rs.getRow();
						rs.first();
						rs.beforeFirst();
						int index = 0;
						if (dimension == Dimension.SIMPLE)
						{
							simple = new String[row];
							while (rs.next())
								simple[index++] = rs.getObject(alias).toString();
						}
						else
						{
							String[] aliases = alias.split(",");
							if (dimension == Dimension.COMPLEX)
							{
								complex = new String[row][aliases.length];
								while (rs.next()) {
									String[] data = new String[aliases.length];
									for (int i = 0; i < aliases.length; i++)
										data[i] = rs.getObject(aliases[i]).toString();
									complex[index++] = data;
								}
							}
							else
							{
								String[] fields = strSQL[0].substring(strSQL[0].indexOf("SELECT ") + 6, strSQL[0].indexOf("FROM")).trim().split(",");
								for (int i = 0; i < fields.length; i++)
									if(fields[i].contains(" AS "))
										fields[i] = fields[i].substring(0, fields[i].indexOf(" AS "));
								if (dimension == Dimension.SIMPLE_MODEL)
								{
									try {
										simpleModel = cls.newInstance();
										while (rs.next())
											for (int i = 0; i < aliases.length; i++)
												CheneyUtils.invokeSet(simpleModel, fields[i], rs.getObject(aliases[i]));
									} catch (InstantiationException | IllegalAccessException e) {
										CheneyUtils.exception(e);
									}
								}
								else if (dimension == Dimension.COMPLEX_MODEL)
								{
									try {
										complexModel = new LinkedList<Object>();
										while (rs.next()) {
											Object model = cls.newInstance();
											for (int i = 0; i < aliases.length; i++)
												CheneyUtils.invokeSet(model, fields[i], rs.getObject(aliases[i]));
											complexModel.add(model);
										}
									} catch (InstantiationException | IllegalAccessException e) {
										CheneyUtils.exception(e);
									}
								}
							}
						}
					}
				}
			} catch (SQLException e) {
				CheneyUtils.exception(e);
				isSuccess = Boolean.FALSE;
				linear = null;
				simple = null;
				complex = null;
				return this;
			}
		}
		CheneyConnectionPool.close(con, rs, pstmt == null ? (stmt == null ? cstmt : stmt) : pstmt);
		return this;
	}
	
	public Boolean getIsSuccess() {
		return isSuccess;
	}
	
	public <M> M getSimpleModel() {
		return (M) simpleModel;
	}
	
	public <M> List<M> getComplexModel() {
		return (List<M>) complexModel;
	}

	public String getLinear() {
		return linear;
	}

	public String[] getSimple() {
		return simple;
	}

	public String[][] getComplex() {
		return complex;
	}
	
	public enum Dimension {
		LINEAR,SIMPLE,COMPLEX,DML,SIMPLE_MODEL,COMPLEX_MODEL,SIMPLE_BATCH,COMPLEX_BATCH,CALL
	}
	
}
