package org.opengeo.gwcdistributed.seed;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.seed.Job;

/**
 * Callable to dispatch a job on each node.
 *
 */
class DoDispatch extends JobDistributedCallable<Object> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 404971908470072126L;
	
	private static Log log = LogFactory.getLog(DistributedTileBreeder.class);

	public DoDispatch(DistributedJob job) {
		super(job);
	}

	public Collection<Object> call() throws Exception {
	    log.trace("Dispatching job "+jobId+" on local breeder");
		assertInit();
		Job j = getBreeder().getJobByID(jobId);
		getBreeder().localDispatchJob(j);
		return null;
	}
}