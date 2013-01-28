package org.opengeo.gwcdistributed.seed;

import java.io.Serializable;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import com.hazelcast.spring.context.SpringAware;

/**
 * Callable which can be distributed across the cluster and which will acquire a
 * reference to the local tile breeder.
 * 
 * @author Kevin Smith, OpenGeo
 *
 * @param <T>
 */
@SpringAware
public abstract class DistributedCallable<T> implements Serializable, Callable<T>{
	static Log log = LogFactory.getLog(DistributedCallable.class);

	protected transient DistributedTileBreeder breeder;

	public DistributedCallable(DistributedTileBreeder breeder) {
		this.breeder=breeder;
	}

	public DistributedTileBreeder getBreeder() {
		return breeder;
	}

	protected void assertInit() {
		Assert.state(breeder!=null, "Breeder was not set");
	}

	/**
	 * Set the breeder to use.  This should be called by Spring exactly once.
	 * @param breeder
	 */
	@Autowired
	public void setBreeder(DistributedTileBreeder breeder) throws GeoWebCacheException {
		log.trace("Aquiring local breeder: "+breeder.toString());
		Assert.state(this.breeder==null, "Breeder can only be set once. This method should only be called by Spring.");
		Assert.notNull(breeder);
		this.breeder=breeder;
	}

}