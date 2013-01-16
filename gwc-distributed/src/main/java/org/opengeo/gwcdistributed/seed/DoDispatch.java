package org.opengeo.gwcdistributed.seed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.Job;
import org.geowebcache.seed.JobNotFoundException;
import org.geowebcache.seed.TaskStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import com.hazelcast.spring.context.SpringAware;

/**
 * Callable to dispatch a job on each node.
 *
 */
@SpringAware class DoDispatch implements Callable<Object>, Serializable {
	final public long jobId;
	transient private DistributedTileBreeder breeder;
	
    private static Log log = LogFactory.getLog(DistributedTileBreeder.class);

	public DoDispatch(long jobID) {
		super();
		this.jobId = jobID;
	}

	public Collection<Object> call() throws Exception {
	    log.trace("Dispatching job "+jobId+" on local breeder");
		
		Assert.state(breeder!=null, "Breeder was not set");
		Job j = breeder.getJobByID(jobId);
		breeder.localDispatchJob(j);
		return null;
	}
	
	/**
	 * Set the breeder to use.  This should be called by Spring exactly once.
	 * @param breeder
	 */
	@Autowired
	public void setBreeder(DistributedTileBreeder breeder){
		log.trace(this.toString()+": Aquiring local breeder: "+breeder.toString());
		Assert.state(this.breeder==null, "Breeder can only be set once. This method should only be called by Spring.");
		Assert.notNull(breeder);
		this.breeder=breeder;
	}
}