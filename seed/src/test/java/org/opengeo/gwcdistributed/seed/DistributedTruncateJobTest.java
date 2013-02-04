package org.opengeo.gwcdistributed.seed;

import static org.junit.Assert.*;

import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.AbstractJobTest;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.seed.Job;
import org.geowebcache.seed.SeedTestUtils;
import org.geowebcache.seed.TileBreeder;
import org.geowebcache.seed.TruncateTask;
import org.geowebcache.storage.TileRangeIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import static org.easymock.EasyMock.expect;
import static org.easymock.classextension.EasyMock.*;
import static org.junit.Assume.*;

public class DistributedTruncateJobTest extends AbstractJobTest {
	
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
	    final TruncateTask task = SeedTestUtils.createMockTruncateTask(breeder);
	    replay(task);
	    replay(breeder);
	    
	    TileLayer tl = createMock(TileLayer.class);
	    replay(tl);

	    DistributedTileRangeIterator dtri = createMock(DistributedTileRangeIterator.class);
	    expect(dtri.nextMetaGridLocation()).andStubDelegateTo(tri);
	    replay(dtri);

	    DistributedTruncateJob job = new DistributedTruncateJob(1, breeder, tl, dtri, false);
	    
	    job.threads[0] = task;

	    return job;
	}

	//@Override
	protected Job jobWithTaskStates(STATE... states) throws Exception {
		assumeTrue(states.length==1); // Tests with multiple tasks don't make sense for Truncate jobs.
		
        hz = Hazelcast.newHazelcastInstance(config);
	    final DistributedTileBreeder breeder = createMock(DistributedTileBreeder.class);
	    final TruncateTask task = SeedTestUtils.createMockTruncateTask(breeder);
	    expect(task.getState()).andReturn(states[0]).anyTimes();
	    expect(breeder.getHz()).andStubReturn(hz);
	    replay(task);
	    replay(breeder);
	    
	    TileLayer tl = createMock(TileLayer.class);
	    replay(tl);
	    DistributedTileRangeIterator tri = createMock(DistributedTileRangeIterator.class);
	    replay(tri);
	    
	    DistributedTruncateJob job = new DistributedTruncateJob(1, breeder, tl, tri, false);
	    
	    return job;
	}

	@Override
	protected TileBreeder createMockTileBreeder() {
		return createMock(DistributedTileBreeder.class);
	}

	//@Override
	protected Job createTestSeedJob(TileBreeder breeder, int threads) {
		assumeTrue(false);
		return null;
	}

	@Override
	protected Job createTestJob(TileBreeder breeder, int threads) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected GWCTask createMockTask(TileBreeder mockBreeder) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
