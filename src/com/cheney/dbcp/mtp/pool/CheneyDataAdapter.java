package com.cheney.dbcp.mtp.pool;

import java.util.concurrent.Callable;

public class CheneyDataAdapter implements Callable<CheneyDataWrap> {

	private CheneyDataWrap wrap;
	
	public CheneyDataAdapter(CheneyDataWrap wrap) {
		this.wrap = wrap;
	}
	
	public CheneyDataWrap call() throws Exception {
		return wrap;
	}
	
}
