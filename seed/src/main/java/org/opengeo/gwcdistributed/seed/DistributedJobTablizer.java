package org.opengeo.gwcdistributed.seed;

import org.apache.commons.lang.ArrayUtils;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.rest.seed.JobTablizer;
import org.geowebcache.seed.JobStatus;
import org.geowebcache.seed.TaskStatus;

import com.hazelcast.core.Member;

public class DistributedJobTablizer extends JobTablizer {
	Column node = new Column("Node"){

		@Override
		public String getField(JobStatus job, TaskStatus task) {
			Member node = ((DistributedTaskStatus)task).getNode();
			return node.toString();
		}};
	
	public DistributedJobTablizer(Appendable doc, TileLayer tl) {
		super(doc, tl);

		this.midColumns = (Column[])ArrayUtils.add(midColumns, node);
	}
	
	

}
