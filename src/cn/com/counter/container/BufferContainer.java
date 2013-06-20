package cn.com.counter.container;

import java.util.Set;

public interface BufferContainer {
	public void put(Long key, Counts counts);
	
	public Counts get(Long key);
	
	public Set<Long> keySet();
}
