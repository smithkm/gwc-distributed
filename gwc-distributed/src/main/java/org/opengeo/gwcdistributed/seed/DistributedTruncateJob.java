package org.opengeo.gwcdistributed.seed;

import org.geowebcache.GeoWebCacheException;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.TruncateJob;
import org.geowebcache.storage.TileRangeIterator;

public class DistributedTruncateJob extends DistributedJob implements TruncateJob {

	protected DistributedTruncateJob(long id, DistributedTileBreeder breeder,
			TileLayer tl, TileRangeIterator tri,
			boolean doFilterUpdate) {
		super(id, breeder, tl, 1, tri, doFilterUpdate);
        threads = new GWCTask[1];
        threads[0] = breeder.createTruncateTask(this);
	}

	public void runSynchronously() throws GeoWebCacheException,
			InterruptedException {
		threads[0].doAction();
	}

}
