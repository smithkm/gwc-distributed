package org.opengeo.gwcdistributed.seed;

import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.TaskStatus;

import com.hazelcast.core.Member;

public class DistributedTaskStatus extends TaskStatus {

	final Member node;
	
	public DistributedTaskStatus(GWCTask task) {
		super(task);
		this.node = ((DistributedTileBreeder)task.getJob().getBreeder()).getNode();
	}

	public Member getNode() {
		return node;
	}
}
