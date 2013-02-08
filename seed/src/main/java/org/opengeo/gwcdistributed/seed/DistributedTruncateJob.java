package org.opengeo.gwcdistributed.seed;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.GWCTask.TYPE;
import org.geowebcache.seed.TruncateJob;

public class DistributedTruncateJob extends DistributedJob implements TruncateJob {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6904431211868719738L;

    public static Log log = LogFactory.getLog(DistributedTruncateJob.class);

	protected DistributedTruncateJob(long id, DistributedTileBreeder breeder,
			TileLayer tl, DistributedTileRangeIterator tri,
			boolean doFilterUpdate) {
		super(id, breeder, tl, 1, tri, doFilterUpdate);
		
		// Only create a task on the originating node
        threads = new GWCTask[1];
        threads[0] = breeder.createTruncateTask(this);
	}

	protected void createTasks(){
        if(threads==null) {
        	log.trace("Not the originating node, not creating truncate tasks.");
        	threads = new GWCTask[0];
        }
	}
	
	@Override
	public TYPE getType() {
		return GWCTask.TYPE.TRUNCATE;
	}

}
