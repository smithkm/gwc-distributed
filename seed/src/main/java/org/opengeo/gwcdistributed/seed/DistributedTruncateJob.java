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
        threads = new GWCTask[1];
	}
	
	@Override
	public TYPE getType() {
		return GWCTask.TYPE.TRUNCATE;
	}

}
