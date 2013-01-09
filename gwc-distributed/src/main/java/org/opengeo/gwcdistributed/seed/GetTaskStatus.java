package org.opengeo.gwcdistributed.seed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.JobNotFoundException;
import org.geowebcache.seed.TaskStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import com.hazelcast.spring.context.SpringAware;

/**
 * Callable to retrieve the status of each task within a job.  It's intended
 *  to be distributed across the cluster using an executor service.
 *
 */
@SpringAware class GetTaskStatus implements Callable<Collection<TaskStatus>>, Serializable {
	final public long jobId;
	transient private DistributedTileBreeder breeder;
	
	public GetTaskStatus(long jobID) {
		super();
		this.jobId = jobID;
	}

	public Collection<TaskStatus> call() throws Exception {
		Assert.state(breeder!=null, "Breeder was not set");
		try{
			GWCTask[] tasks = breeder.getJobByID(jobId).getTasks();
			List<TaskStatus> results = new ArrayList<TaskStatus>(tasks.length);
			for(GWCTask task: tasks) {
				results.add(task.getStatus());
			}
			return results;
		} catch (JobNotFoundException ex) {
			return Collections.emptyList();
		}
	}
	
	/**
	 * Set the breeder to use.  This should be called by Spring exactly once.
	 * @param breeder
	 */
	@Autowired
	public void setBreeder(DistributedTileBreeder breeder){
		Assert.state(breeder==null, "Breeder can only be set once. This method should only be called by Spring.");
		Assert.notNull(breeder);
		this.breeder=breeder;
	}
}