package org.opengeo.gwcdistributed.seed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.TaskStatus;

/**
 * Callable to retrieve the status of each task within a job.  It's intended
 *  to be distributed across the cluster using an executor service.
 *
 */
class GetTaskStatus extends JobDistributedCallable<Collection<TaskStatus>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -164838784678432156L;

	public GetTaskStatus(DistributedJob job) {
		super(job);
	}

	public Collection<TaskStatus> call() throws Exception {
		assertInit();
		if(getJob()==null){
			return Collections.emptyList();
		} else {
			GWCTask[] tasks = getJob().getTasks();
			List<TaskStatus> results = new ArrayList<TaskStatus>(tasks.length);
			for(GWCTask task: tasks) {
				results.add(new DistributedTaskStatus(task));
			}
			return results;
		}
	}
	
}