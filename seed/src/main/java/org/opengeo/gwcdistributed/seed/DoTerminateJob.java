package org.opengeo.gwcdistributed.seed;

/**
 * Calls terminateLocal on the given job.
 * @author smithkm
 *
 */
public class DoTerminateJob extends JobDistributedCallable<Object> {

	public DoTerminateJob(DistributedJob job) {
		super(job);
	}

	public Object call() throws Exception {
		getJob().terminateLocal();
		return null;
	}

}
