package cn.com.counter.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;

import cn.com.counter.container.BaseContainer;
import cn.com.counter.container.BufferContainer;
import cn.com.counter.container.Counts;

/**
 * @author yyq
 */
public class Counter {

	private static DataSource ds = null;
	private static BufferContainer counterContainer = new BaseContainer();
	private static final String TABLE_NAME = "bm_counter";
	private static Long MAX_BUFFER_COUNT = 20L;
	
	/**
	 * 初始化数据源
	 * @param dataSource
	 */
	public static void initDataSource(DataSource dataSource){
		ds = dataSource;
	}
	
	/**
	 * 每个缓存的最大存储次数，默认为20
	 * @param count
	 */
	public static void initMaxBufferCount(Long count){
		MAX_BUFFER_COUNT = count;
	}
	
	/**
	 * 设置存储计数的缓存容器。
	 * 只需设置一次
	 * @param container
	 */
	public static void setBufferContainer(BufferContainer container){
		counterContainer = container;
	}
	
	/**
	 * 初始化counter一行数据
	 * @return 刚初始化的主键，为0则初始化失败
	 */
	public long initOneCounter(){
		checkIfDataSourceInit();
		
		QueryRunner qr = new QueryRunner();
		Connection conn = null;
		Date current = new Date();
		String sql = "INSERT INTO "+TABLE_NAME+"(counts, today, this_week, this_month, yesterday, last_week, last_month, create_at, update_at) VALUES(?,?,?,?,?,?,?,?,?)";
		Object[] objs = new Object[]{0, 0, 0, 0, 0, 0, 0, current, current};
		try {
			conn = ds.getConnection();
			qr.update(conn, sql, objs);
			if(sql.indexOf("INSERT INTO")>-1){

				String sqlTmp = "SELECT LAST_INSERT_ID() AS id";
				Map<String, Object> map = qr.query(conn, sqlTmp, new MapHandler());
				long id = Long.parseLong( map.get( "id" )+"" );
				return id;
			}
		}  
		catch (SQLException e) {
			e.printStackTrace();
		}
		finally{
			close( conn );
		}
		return 0L;
	}
	
	/**
	 * 计数次数增长
	 * @param counterId
	 */
	public void incCounts(long counterId){
		checkIfDataSourceInit();
		
		Counts counts = findCounts( counterId );
		if(counts!=null){
			updateCounts( counterId );
		}
	}
	
	/**
	 * 获取指定counterId的次数，默认container在负载均衡的环境中无法正常使用
	 * @param counterId
	 * @return
	 */
	public long getCurrentCounts(long counterId){
		return getCurrentCounts( counterId, true );
	}
	
	/**
	 * 获取指定counterId的次数
	 * @param counterId
	 * @param containBuffer false时， 默认container在负载均衡的环境中无法正常使用
	 * @return
	 */
	public long getCurrentCounts(long counterId, boolean containBuffer){
		checkIfDataSourceInit();
		
		Counts counts = findCounts( counterId );
		if(counts==null){
			return 0;
		}
		if(containBuffer){
			return counts.getDbCounts()+counts.getBufferCounts();
		}else{
			return counts.getDbCounts();
		}
		
	}
	
	/**
	 * 更新所有缓存数据到数据库
	 */
	public void updateAllBuffer2Db(){
		checkIfDataSourceInit();
		
		Set<Long> sets = counterContainer.keySet();
		for( Long key : sets) {
	        updateCounts2Db( key );
        }
	}
	
	/**
	 * 更新数据库或者缓存数据
	 * @param counterId
	 * @param counts
	 */
	private void updateCounts(long counterId){
		Counts counts = counterContainer.get( counterId );
		synchronized( counts ) {
			if(counts.getBufferCounts()>=(MAX_BUFFER_COUNT-1)){
				counts.setBufferCounts( counts.getBufferCounts()+1 );
				updateCounts2Db( counterId );
				
				Map<String, Object> counterInDb = findConutsInDb( counterId );
	        	counts.setBufferCounts( 0 );	//当前次数
	        	counts.setDbCounts( Long.parseLong( counterInDb.get( "counts" )+"" ) );
			}else{
				counts.setBufferCounts( counts.getBufferCounts()+1 );
			}
        }
	}
	
	/**
	 * 更新数据到数据库
	 * @param counterId
	 * @param counts
	 */
	private void updateCounts2Db(long counterId ){
		Counts counts = counterContainer.get( counterId );
		String sql = "UPDATE "+TABLE_NAME+" SET counts=counts+?, today=today+?, update_at=? WHERE id=?";
		QueryRunner qr = new QueryRunner( ds );
		long addNum = counts.getBufferCounts();
		try {
	        qr.update( sql, addNum, addNum, new Date(), counterId );
        }
        catch( SQLException e ) {
        	System.out.println("【updateCounts2Db】更新失败");
	        e.printStackTrace();
        }
        
	}
	
	/**
	 * 从map或者db中找counter数据
	 * @param counterId
	 * @return
	 */
	private Counts findCounts(long counterId){

		Counts counts = counterContainer.get( counterId );
		if(counts==null){
			Map<String, Object> counterInDb = findConutsInDb( counterId );
	        if(counterInDb!=null){
	        	counts = new Counts();
	        	counts.setBufferCounts( 0 );
	        	counts.setDbCounts( Long.parseLong( counterInDb.get( "counts" )+"" ) );
	        }
		}
		if(counts==null){
			System.out.println("id="+counterId+" 的counter数据不存在");
		}else{
			counterContainer.put( counterId, counts );
		}
		return counts;
	}
	
	/**
	 * 找出counter中指定id的数据
	 * @param counterId
	 * @return
	 */
	public Map<String, Object> findConutsInDb(long counterId){
		String sql = "SELECT * FROM "+TABLE_NAME+" WHERE id=?";
		QueryRunner qr = new QueryRunner(ds);
		Map<String, Object> counterInDb = null;
		try {
	        counterInDb = qr.query( sql, new MapHandler(), counterId );
        }
        catch( SQLException e ) {
	        e.printStackTrace();
        }
        return counterInDb;
	}
	
	private void close(Connection connection){
		try {
            DbUtils.close( connection );
        }
        catch( SQLException e ) {
            e.printStackTrace();
        }
	}
	
	private void checkIfDataSourceInit(){
		if(ds==null){
			throw new RuntimeException( "请先配置数据源，调用cn.com.counter.core.Counter 的initDataSource进行配置" );
		}
	}
	
	public static void main( String[] args ) {
	    Map<Long, Counts> tmp = new HashMap<Long, Counts>();
	    Counts counts = new Counts();
	    counts.setBufferCounts( 10 );
	    counts.setDbCounts( 10 );
	    tmp.put( 1L, counts );
	    counts.setBufferCounts( counts.getBufferCounts()+1 );
	    
	    System.out.println(counts.getBufferCounts());
	    System.out.println(tmp.get( 1L ).getBufferCounts());
    }
}
