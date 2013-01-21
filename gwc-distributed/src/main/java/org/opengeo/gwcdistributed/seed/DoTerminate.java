package org.opengeo.gwcdistributed.seed;

/**
 * Calls terminateLocal on the given job.
 * @author smithkm
 *
 */
public class DoTerminate extends JobDistributedCallable<Object> {

	public DoTerminate(DistributedJob job) {
		super(job);
	}

	public Object call() throws Exception {
		getJob().terminateLocal();
		return null;
	}

}
