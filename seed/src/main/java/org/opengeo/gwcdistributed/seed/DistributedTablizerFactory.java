package org.opengeo.gwcdistributed.seed;

import org.geowebcache.layer.TileLayer;
import org.geowebcache.rest.seed.JobTablizer;
import org.geowebcache.rest.seed.TablizerFactory;

public class DistributedTablizerFactory extends TablizerFactory {

	@Override
	public JobTablizer getJobTablizer(Appendable doc, TileLayer tl) {
		return new DistributedJobTablizer(doc, tl);
	}
	
}
