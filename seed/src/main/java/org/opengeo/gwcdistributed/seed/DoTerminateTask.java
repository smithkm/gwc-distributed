package org.opengeo.gwcdistributed.seed;

/**
 * Kills the specified task, wherever it is in the cluster.
 * @author Kevin Smith, OpenGeo
 *
 */
public class DoTerminateTask extends DistributedCallable<Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1633379226901413347L;
	long taskId;
	
	public DoTerminateTask(DistributedTileBreeder breeder, long taskId) {
		super(breeder);
		this.taskId = taskId;
	}

	public Boolean call() throws Exception {
		assertInit();
		return breeder.terminateLocalGWCTask(taskId);
	}

}
