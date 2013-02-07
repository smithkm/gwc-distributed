package org.opengeo.gwcdistributed.seed;

import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.GWCTask.TYPE;
import org.geowebcache.seed.TruncateJob;

public class DistributedTruncateJob extends DistributedJob implements TruncateJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6904431211868719738L;

	protected DistributedTruncateJob(long id, DistributedTileBreeder breeder,
			TileLayer tl, DistributedTileRangeIterator tri,
			boolean doFilterUpdate) {
		super(id, breeder, tl, 1, tri, doFilterUpdate);
		
		// Create a task only on the originating node so this happens once.
		// Truncating can't be broken across nodes/threads up as it is currently implemented.
		threads = new GWCTask[1];
		threads[0] = breeder.createTruncateTask(this);
	}

	protected void createTasks(){
		// Actually create the thread in the constructor so only one is created.
        if(threads==null) threads = new GWCTask[0];
	}
	
	@Override
	public TYPE getType() {
		return GWCTask.TYPE.TRUNCATE;
	}

}
