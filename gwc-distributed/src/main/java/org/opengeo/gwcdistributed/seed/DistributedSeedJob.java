package org.opengeo.gwcdistributed.seed;

import org.geowebcache.layer.TileLayer;
import org.geowebcache.storage.TileRangeIterator;

public class DistributedSeedJob extends DistributedJob {

	protected DistributedSeedJob(long id, DistributedTileBreeder breeder,
			TileLayer tl, int threadCount, TileRangeIterator tri,
			boolean doFilterUpdate) {
		super(id, breeder, tl, threadCount, tri, doFilterUpdate);
		// TODO Auto-generated constructor stub
	}

}
