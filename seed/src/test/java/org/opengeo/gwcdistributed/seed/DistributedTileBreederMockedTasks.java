package org.opengeo.gwcdistributed.seed;

import java.util.Iterator;

import org.geowebcache.seed.SeedJob;
import org.geowebcache.seed.SeedTask;
import org.geowebcache.seed.TruncateJob;
import org.geowebcache.seed.TruncateTask;

import com.hazelcast.core.HazelcastInstance;

import static org.easymock.classextension.EasyMock.*;

/**
 * DistributedTileBreeder with its task factory methods overridden to use 
 * iterators.
 * 
 * This allows mock tasks to be injected into the breeder to test the other 
 * parts of its functionality.  It's not pretty but it works.
 */
public class DistributedTileBreederMockedTasks extends DistributedTileBreeder {

	public DistributedTileBreederMockedTasks(HazelcastInstance hz) {
		super(hz);
	}

	Iterator<SeedTask> seedIt;
	Iterator<TruncateTask> truncIt;
	
	@Override
	protected SeedTask createSeedTask(SeedJob job) {
		SeedTask task = seedIt.next();
		expect(task.getJob()).andStubReturn(job);
		replay(task);
		return task;
	}

	@Override
	protected TruncateTask createTruncateTask(TruncateJob job) {
		TruncateTask task = truncIt.next();
		expect(task.getJob()).andStubReturn(job);
		replay(task);
		return task;
	}

	
}
