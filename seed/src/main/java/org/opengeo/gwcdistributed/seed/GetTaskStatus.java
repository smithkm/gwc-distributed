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
class GetTaskStatus extends JobDistributedCallable<Collection<TaskStatus>> {
	
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
				results.add(task.getStatus());
			}
			return results;
		}
	}
	
}