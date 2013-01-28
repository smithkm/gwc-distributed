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

import com.hazelcast.spring.context.SpringAware;

/**
 * Callable to do something to a job and which can be distributed via hazelcast.
 *
 */
public abstract class JobDistributedCallable<T> extends DistributedCallable<T> {
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