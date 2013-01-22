package org.opengeo.gwcdistributed.seed;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.seed.Job;

import org.springframework.util.Assert;

import com.hazelcast.spring.context.SpringAware;

/**
 * Callable to dispatch a job on each node.
 *
 */
@SpringAware class DoDispatch extends JobDistributedCallable<Object> {
    private static Log log = LogFactory.getLog(DistributedTileBreeder.class);

	public DoDispatch(DistributedJob job) {
		super(job);
	}

	public Collection<Object> call() throws Exception {
	    log.trace("Dispatching job "+jobId+" on local breeder");
		
		Assert.state(getBreeder()!=null, "Breeder was not set");
		Job j = getBreeder().getJobByID(jobId);
		getBreeder().localDispatchJob(j);
		return null;
	}
}