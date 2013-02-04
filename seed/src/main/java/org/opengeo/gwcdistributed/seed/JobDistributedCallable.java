package org.opengeo.gwcdistributed.seed;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Callable to do something to a job and which can be distributed via hazelcast.
 *
 */
public abstract class JobDistributedCallable<T> extends DistributedCallable<T> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5138834220019980045L;
	
	final public long jobId;
	transient DistributedJob job;
	static Log log = LogFactory.getLog(JobDistributedCallable.class);
    
	public JobDistributedCallable(DistributedJob job) {
		super((DistributedTileBreeder) job.getBreeder());
		this.job = job;
		this.jobId = job.getId();
	}
	
	public DistributedJob getJob() {
		return job;
	}
	
	@Autowired
	public void setBreeder(DistributedTileBreeder breeder) throws GeoWebCacheException {
		super.setBreeder(breeder);
		this.job=(DistributedJob) breeder.getJobByID(jobId);
	}

}