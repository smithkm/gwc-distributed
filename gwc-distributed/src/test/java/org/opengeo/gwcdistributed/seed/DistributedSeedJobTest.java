package org.opengeo.gwcdistributed.seed;

import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.AbstractJobTest;
import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.seed.Job;
import org.geowebcache.seed.SeedTask;
import org.geowebcache.seed.TileBreeder;
import org.geowebcache.storage.TileRangeIterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import static org.easymock.classextension.EasyMock.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("distributedSeedJobTest-context.xml")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class DistributedSeedJobTest extends AbstractJobTest {
	@Autowired
	HazelcastInstance hzInstance;
	@Autowired
	DistributedTileBreeder breeder;

	@Before
    public void setUp() {
    }
	
	@After
	public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
    }

	@Override
	protected Job initNextLocation(TileRangeIterator tri) throws Exception {
		final DistributedTileBreeder breeder = (DistributedTileBreeder) createMockTileBreeder();
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

        final DistributedTileBreeder breeder = (DistributedTileBreeder) createMockTileBreeder();

        for(STATE state: states){
		    final SeedTask task = createMockSeedTask(breeder);
		    expect(task.getState()).andStubReturn(state);
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

	@Override
	protected TileBreeder createMockTileBreeder() {
		reset(breeder);
		expect(breeder.getHz()).andStubReturn(hzInstance);
	    return breeder;
	}

	@Override
	protected Job createTestSeedJob(TileBreeder breeder, int threads) {
        TileLayer tl = createMock(TileLayer.class);
        replay(tl);
        TileRangeIterator tri = createMock(TileRangeIterator.class);
        replay(tri);
        return new DistributedSeedJob(1l, (DistributedTileBreeder) breeder, tl, threads, tri, false);
	}

}
