package org.opengeo.gwcdistributed.seed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.JobNotFoundException;
import org.geowebcache.seed.TaskStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import com.hazelcast.spring.context.SpringAware;

/**
 * Callable to do something to a job and which can be distributed via hazelcast.
 *
 */
@SpringAware 
public abstract class JobDistributedCallable<T> implements Callable<T>, Serializable {
	final public long jobId;
	transient private DistributedJob job;
	transient private DistributedTileBreeder breeder;
	
    private static Log log = LogFactory.getLog(JobDistributedCallable.class);
    
	public JobDistributedCallable(DistributedJob job) {
		super();
		this.job = job;
		this.jobId = job.getId();
		this.breeder = (DistributedTileBreeder) job.getBreeder();
	}
	
	public DistributedJob getJob() {
		return job;
	}

	public DistributedTileBreeder getBreeder() {
		return breeder;
	}

	protected void assertInit(){
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
		this.job=(DistributedJob) breeder.getJobByID(jobId);
	}
}