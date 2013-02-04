package org.opengeo.gwcdistributed.seed;

import org.geowebcache.GeoWebCacheException;
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
	}

	protected void createTasks(){
		// FIXME truncate should actually only happen once per cluster 
        threads = new GWCTask[1];
        threads[0] = breeder.createTruncateTask(this);
	}
	
	public void runSynchronously() throws GeoWebCacheException,
			InterruptedException {
		threads[0].doAction();
	}

	@Override
	public TYPE getType() {
		return GWCTask.TYPE.TRUNCATE;
	}

}
