package cn.com.counter.container;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BaseContainer implements BufferContainer{

	private Map<Long, Counts> map = new HashMap<Long, Counts>();
	
	@Override
    public void put( Long key, Counts value ) {
		map.put( key, value );
    }

	@Override
    public Counts get( Long key ) {		
	    return map.get( key );
    }

	public Set<Long> keySet(){
		return map.keySet();
	}
}
