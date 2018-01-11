package com.cheney.dbcp.mtp.pool;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.cheney.dbcp.mtp.annotation.FieldDecorate;
import com.cheney.dbcp.mtp.annotation.TypeDecorate;
import com.cheney.dbcp.mtp.pool.CheneyDataWrap.Dimension;
import com.cheney.dbcp.mtp.utils.CheneyUtils;

public final class CheneyThreadPool {
	
	private static ExecutorService executorService = Executors.newFixedThreadPool(CheneyUtils.getIntegerValue("threadPoolSize"));
	
	public static CheneyDataWrap submit(Dimension dimension, Object[] params, Class<?> cls, String... strSQL) {
		try {
			return executorService.submit(new CheneyDataAdapter(new CheneyDataWrap(params, strSQL).handler(dimension, cls))).get();
		} catch (InterruptedException | ExecutionException e) {
			CheneyUtils.exception(e);
		}
		return null;
	}
	
	public static CheneyDataWrap submit(Dimension dimension, Object param, Class<?> cls, String... strSQL) {
		return submit(dimension, new Object[]{ param }, cls, strSQL);
	}
	
	public static CheneyDataWrap submit(Dimension dimension, Class<?> cls, String... strSQL) {
		return submit(dimension, (Object[])null, cls, strSQL);
	}
	
	public static CheneyDataWrap submit(Object[] params, Dimension dimension, String... strSQL) {
		return submit(dimension, params, (Class<?>)null, strSQL);
	}
	
	public static CheneyDataWrap submit(Object param, Dimension dimension, String... strSQL) {
		return submit(dimension, param, (Class<?>)null, strSQL);
	}
	
	public static CheneyDataWrap submit(Dimension dimension, String... strSQL) {
		return submit(dimension, (Class<?>)null, strSQL);
	}
	
	public static String submitLinear(String strSQL, Object[] params) {
		return submit(params, Dimension.LINEAR, strSQL).getLinear();
	}
	
	public static String submitLinear(String strSQL, Object param) {
		return submitLinear(strSQL, new Object[]{ param });
	}
	
	public static String submitLinear(String strSQL) {
		return submitLinear(strSQL, (Object[])null);
	}
	
	public static String[] submitSimple(String strSQL, Object[] params) {
		return submit(params, Dimension.SIMPLE, strSQL).getSimple();
	}
	
	public static String[] submitSimple(String strSQL, Object param) {
		return submitSimple(strSQL, new Object[]{ param });
	}
	
	public static String[] submitSimple(String strSQL) {
		return submitSimple(strSQL, (Object[])null);
	}

	public static String[][] submitComplex(String strSQL, Object[] params) {
		return submit(params, Dimension.COMPLEX, strSQL).getComplex();
	}
	
	public static String[][] submitComplex(String strSQL, Object param) {
		return submitComplex(strSQL, new Object[]{ param });
	}
	
	public static String[][] submitComplex(String strSQL) {
		return submitComplex(strSQL, (Object[])null);
	}
	
	public static <M> M submitSimpleModel(String strSQL, Class<?> cls, Object[] params) {
		return submit(Dimension.SIMPLE_MODEL, params, cls, strSQL).getSimpleModel();
	}
	
	public static <M> M submitSimpleModel(String strSQL, Class<?> cls, Object param) {
		return submitSimpleModel(strSQL, cls, new Object[]{ param });
	}
	
	public static <M> M submitSimpleModel(String strSQL, Class<?> cls) {
		return submitSimpleModel(strSQL, cls, (Object[])null);
	}

	public static <M> M submitSimpleModel(Class<?> cls, Object priKey) {
		StringBuilder strSQL = new StringBuilder("SELECT ");
		Field[] fields = cls.getDeclaredFields();
		String key = null;
		for (Field field : fields) {
			if(field.getAnnotation(FieldDecorate.class).key())
				key = field.getName();
			strSQL.append(field.getName()).append(",");
		}
		strSQL.setCharAt(strSQL.length() - 1, ' ');
		strSQL.append("FROM ").append(cls.getAnnotation(TypeDecorate.class).table());
		strSQL.append(" WHERE ").append(key).append("=?");
		return submitSimpleModel(strSQL.toString(), cls, priKey);
	}
	
	public static <M> List<M> submitComplexModel(String strSQL, Class<?> cls, Object[] params) {
		return submit(Dimension.COMPLEX_MODEL, params, cls, strSQL).getComplexModel();
	}
	
	public static <M> List<M> submitComplexModel(String strSQL, Class<?> cls, Object param) {
		return submitComplexModel(strSQL, cls, new Object[]{ param });
	}
	
	public static <M> List<M> submitComplexModel(String strSQL, Class<?> cls) {
		return submitComplexModel(strSQL, cls, (Object[])null);
	}
	
	public static <M> List<M> submitComplexModel(Class<?> cls) {
		StringBuilder strSQL = new StringBuilder("SELECT ");
		Field[] fields = cls.getDeclaredFields();
		for (Field field : fields)
			strSQL.append(field.getName()).append(",");
		strSQL.setCharAt(strSQL.length() - 1, ' ');
		strSQL.append("FROM ").append(cls.getAnnotation(TypeDecorate.class).table());
		return submitComplexModel(strSQL.toString(), cls);
	}
	
	public static Boolean submitSimpleBatch(String strSQL, Object[] params) {
		return submit(params, Dimension.SIMPLE_BATCH, strSQL).getIsSuccess();
	}
	
	public static Boolean submitComplexBatch(String... strSQL) {
		return submit(Dimension.COMPLEX_BATCH, strSQL).getIsSuccess();
	}
	
	public static Boolean submitDML(String strSQL, Object[] params) {
		return submit(params, Dimension.DML, strSQL).getIsSuccess();
	}
	
	public static Boolean submitDML(String strSQL, Object param) {
		return submitDML(strSQL, new Object[]{ param });
	}
	
	public static Boolean submitDML(String strSQL) {
		return submitDML(strSQL, (Object[])null);
	}
	
	public static Boolean submitSave(Object entity) {
		Class<?> cls = entity.getClass();
		LinkedList<Object> param = new LinkedList<Object>();
		StringBuilder strSQL = new StringBuilder("INSERT INTO ");
		strSQL.append(cls.getAnnotation(TypeDecorate.class).table()).append("(");
		int countNotIncField = 0;
		Field[] fs = cls.getDeclaredFields();
		for (Field f : fs) {
			if(f.getAnnotation(FieldDecorate.class).inc())
				continue;
			countNotIncField++;
			strSQL.append(f.getName() + ",");
			param.add(CheneyUtils.invokeGet(entity, f.getName()));
		}
		if (countNotIncField == 0)
			return Boolean.FALSE;
		strSQL.setCharAt(strSQL.length() - 1, ')');
		strSQL.append(" VALUES(");
		for (int i = 0; i < countNotIncField; i++)
			strSQL.append("?,");
		strSQL.setCharAt(strSQL.length() - 1, ')');
		return submitDML(strSQL.toString(), param.toArray());
	}
	
	public static Boolean submitDelete(Class<?> cls, Object priKey) {
		StringBuilder strSQL = new StringBuilder("DELETE FROM ");
		strSQL.append(cls.getDeclaredAnnotation(TypeDecorate.class).table());
		strSQL.append(" WHERE ");
		Field[] fields = cls.getDeclaredFields();
		for (Field field : fields)
			if (field.getAnnotation(FieldDecorate.class).key()) {
				strSQL.append(field.getName()).append("=?");
				break;
			}
		return submitDML(strSQL.toString(), priKey);
	}
	
	public static Boolean submitDelete(Object entity) {
		Class<?> c = entity.getClass();
		Field[] fields = c.getDeclaredFields();
		StringBuilder strSQL = new StringBuilder("DELETE FROM ");
		strSQL.append(c.getAnnotation(TypeDecorate.class).table());
		strSQL.append(" WHERE ");
		Object priKeyValue = null;
		for (Field field : fields)
			if(field.getAnnotation(FieldDecorate.class).key()) {
				strSQL.append(field.getName()).append("=?");
				priKeyValue = CheneyUtils.invokeGet(entity, field.getName());
				break;
			}
		return submitDML(strSQL.toString(), priKeyValue);
	}

	public static Boolean submitUpdate(Object entity, String fieldNameUseDotCat) {
		Class<?> c = entity.getClass();
		LinkedList<Object> param = new LinkedList<Object>();
		String key = null;
		Object priKeyValue = null;
		Field[] fields = c.getDeclaredFields();
		for (Field field : fields) {
			if(field.getAnnotation(FieldDecorate.class).key()) {
				key = field.getName();
				priKeyValue = CheneyUtils.invokeGet(entity, key);
				break;
			}
			if(field.getAnnotation(FieldDecorate.class).inc())
				break;
		}
		StringBuilder strSQL = new StringBuilder("UPDATE ");
		strSQL.append(c.getAnnotation(TypeDecorate.class).table());
		strSQL.append(" SET ");
		String[] fieldNames = fieldNameUseDotCat.split("\\.");
		for(String fname : fieldNames) {
			param.add(CheneyUtils.invokeGet(entity, fname));
			strSQL.append(fname).append("=?,");
		}
		strSQL.setCharAt(strSQL.length() - 1, ' ');
		strSQL.append("WHERE ");
		strSQL.append(key).append("=?");
		param.add(priKeyValue);
		return submitDML(strSQL.toString(), param.toArray());
	}
	
	public static Boolean submitUpdate(Object entity) {
		Class<?> c = entity.getClass();
		LinkedList<Object> params = new LinkedList<Object>();
		String key = null;
		Object priKeyValue = null;
		Field[] fields = c.getDeclaredFields();
		StringBuilder strSQL = new StringBuilder("UPDATE ");
		strSQL.append(c.getAnnotation(TypeDecorate.class).table());
		strSQL.append(" SET ");
		for (Field field : fields) {
			if(field.getAnnotation(FieldDecorate.class).key()) {
				key = field.getName();
				priKeyValue = CheneyUtils.invokeGet(entity, key);
				continue;
			}
			if(field.getAnnotation(FieldDecorate.class).inc())
				continue;
			params.add(CheneyUtils.invokeGet(entity, field.getName()));
			strSQL.append(field.getName()).append("=?,");
		}
		strSQL.setCharAt(strSQL.length() - 1, ' ');
		strSQL.append("WHERE ");
		strSQL.append(key).append("=?");
		params.add(priKeyValue);
		return submitDML(strSQL.toString(), params.toArray());
	}
	
	public static void shutdown() {
		executorService.shutdown();
	}
	
}
