package org.opengeo.gwcdistributed.seed;

import org.geowebcache.GeoWebCacheException;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.SeedJob;
import org.geowebcache.seed.TileRequest;
import org.geowebcache.storage.TileRangeIterator;

public class DistributedSeedJob extends DistributedJob implements SeedJob{

	protected DistributedSeedJob(long id, DistributedTileBreeder breeder,
			TileLayer tl, int threadCount, TileRangeIterator tri,
			boolean doFilterUpdate) {
		super(id, breeder, tl, threadCount, tri, doFilterUpdate);
		// TODO Auto-generated constructor stub
		threads = new GWCTask[threadCount];
	}

	public void failure(GWCTask task, TileRequest request, Exception e)
			throws GeoWebCacheException {
		// TODO Auto-generated method stub
		
	}

	public long totalFailuresBeforeAborting() {
		// TODO Auto-generated method stub
		return 0;
	}

	public int tileFailureRetryCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long tileFailureRetryWaitTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	public long getFailures() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isReseed() {
		// TODO Auto-generated method stub
		return false;
	}

}
