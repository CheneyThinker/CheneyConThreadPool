package com.cheney.dbcp.mtp.utils;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;
import java.util.ResourceBundle;

import com.cheney.dbcp.mtp.pool.CheneyConnectionPool;
import com.cheney.dbcp.mtp.pool.CheneyThreadPool;

public final class CheneyUtils {

	private static final ResourceBundle bundle = ResourceBundle.getBundle("cheney", Locale.CHINA);
	
	public static void exception(Exception e) {
		StringBuilder builder = new StringBuilder(e.getMessage());
		StackTraceElement[] elements = e.getStackTrace();
		for (int i = 0; i < elements.length; i++) {
			StackTraceElement element = elements[i];
			builder.append("\n\tFile:  ")
				   .append(element.getFileName())
				   .append("\n\tClass: ")
				   .append(element.getClassName())
				   .append("\n\tMethod:")
				   .append(element.getMethodName())
				   .append("\n\tLine:  ")
				   .append(element.getLineNumber());
		}
		System.err.println(builder.toString());
	}
	
	public static void release() {
		CheneyConnectionPool.release();
		CheneyThreadPool.shutdown();
	}
	
	public static String getStringValue(String key) {
		return bundle.getString(key);
	}
	
	public static Integer getIntegerValue(String key) {
		return Integer.valueOf(getStringValue(key));
	}
	
	public static Float getFloatValue(String key) {
		return Float.valueOf(getStringValue(key));
	}
	
	public static void handleParams(PreparedStatement pstmt, Object[] params) {
		try {
			if(params != null)
				for (int i = 0; i < params.length; i++)
					pstmt.setObject(i + 1, params[i]);
		} catch (SQLException e) {
			exception(e);
		}
	}
	
	public static void handleParams(PreparedStatement pstmt, Object params) {
		try {
			if(params != null)
				pstmt.setObject(1, params);
		} catch (SQLException e) {
			exception(e);
		}
	}
	
	public static void handleParams(PreparedStatement pstmt, int index, Object params) {
		try {
			if(params != null)
				pstmt.setObject(index + 1, params);
		} catch (SQLException e) {
			exception(e);
		}
	}
	
	public static Object invokeGet(Object obj, String fieldName) {
		try {
			Class<?> beanClass = obj.getClass();
			PropertyDescriptor pd = new PropertyDescriptor(fieldName, beanClass);
			Method method = pd.getReadMethod();
			Object object = null;
			if (pd != null)
				object = method.invoke(obj);
			return object;
		} catch (IntrospectionException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			exception(e);
			return null;
		}
	}
	
	public static void invokeSet(Object obj, String fieldName, Object fieldValue) {
		try {
			if(fieldValue != null) {
				Class<?> beanClass = obj.getClass();
				PropertyDescriptor pd = new PropertyDescriptor(fieldName, beanClass);
				Method m = pd.getWriteMethod();
				m.invoke(obj, fieldValue);
			}
		} catch (SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | IntrospectionException e) {
			exception(e);
		}
	}
	
	public static Object invokeGet(String fieldName, Object obj) {	
		try {
			Class<?> c = obj.getClass();
			Method m = c.getDeclaredMethod("get" + firstChar2UpperCase(fieldName));
			return m.invoke(obj);
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			exception(e);
			return null;
		}
	}
	
	public static void invokeSet(String fieldName, Object fieldValue, Object obj){
		try {
			if(fieldValue != null) {
				Method m = obj.getClass().getDeclaredMethod("set" + firstChar2UpperCase(fieldName), fieldValue.getClass());
				m.invoke(obj, fieldValue);
			}
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
			exception(e);
		}
	}
	
	public static int getCharCount(char c, String text) {
		char[] ch = text.toCharArray();
		int j = 0;
		for (int i = 0; i < ch.length; i++)
			if (ch[i] == c)
				j++;
		return j;
	}
	
	public static String firstChar2UpperCase(String str) {
		return Character.toUpperCase(str.charAt(0)) + str.substring(1);
	}
	
}
