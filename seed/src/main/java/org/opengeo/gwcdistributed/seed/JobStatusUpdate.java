package org.opengeo.gwcdistributed.seed;

import org.geowebcache.seed.JobStatus;

/**
 * Propagates a new JobStatus for a Job.
 * @author smithkm
 *
 */
public class JobStatusUpdate extends JobDistributedCallable<Object> {

	final JobStatus status;
	
	public JobStatusUpdate(DistributedJob job, JobStatus status) {
		super(job);
		this.status = status;
	}

	public Object call() throws Exception {
		getJob().cachedStatus=status;
		return null;
	}

}
