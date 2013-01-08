package org.opengeo.gwcdistributed.seed;

import static org.junit.Assert.*;

import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.AbstractJobTest;
import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.seed.Job;
import org.geowebcache.seed.SeedTask;
import org.geowebcache.storage.TileRangeIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import static org.easymock.classextension.EasyMock.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

public class DistributedSeedJobTest extends AbstractJobTest {
	HazelcastInstance hz;
	Config config;

	@Before
    public void setUp() {
		if(config==null)
			config = new Config();
    }
	
	@After
	public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
    }

	@Override
	protected Job initNextLocation(TileRangeIterator tri) throws Exception {
	    final DistributedTileBreeder breeder = createMock(DistributedTileBreeder.class);
	    final SeedTask task = createMockSeedTask(breeder);
	    replay(task);
	    replay(breeder);
	    
	    TileLayer tl = createMock(TileLayer.class);
	    replay(tl);
	    
	    DistributedTruncateJob job = new DistributedTruncateJob(1, breeder, tl, tri, false);
	    
	    job.threads[0] = task;

	    return job;
	}

	@Override
	protected Job jobWithTaskStates(STATE... states) throws Exception {
        hz = Hazelcast.newHazelcastInstance(config);

        final DistributedTileBreeder breeder = createMock(DistributedTileBreeder.class);

        for(STATE state: states){
		    final SeedTask task = createMockSeedTask(breeder);
		    expect(task.getState()).andStubReturn(state);
		    expect(breeder.getHz()).andStubReturn(hz);
		    replay(task);
	    }
	    replay(breeder);

	    TileLayer tl = createMock(TileLayer.class);
	    replay(tl);
	    TileRangeIterator tri = createMock(TileRangeIterator.class);
	    replay(tri);
		DistributedSeedJob job = new DistributedSeedJob(1, breeder, tl, states.length, tri, false);
	    
	    return job;
	}

}
