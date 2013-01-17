package org.opengeo.gwcdistributed.seed;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.easymock.IAnswer;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.AbstractJobTest;
import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.seed.Job;
import org.geowebcache.seed.SeedTask;
import org.geowebcache.seed.SeedTestUtils;
import org.geowebcache.seed.TaskStatus;
import org.geowebcache.seed.TileBreeder;
import org.geowebcache.storage.TileRange;
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
	protected Job initNextLocation(final TileRangeIterator tri) throws Exception {
		final DistributedTileBreeder breeder = (DistributedTileBreeder) createMockTileBreeder();
	    final SeedTask task = SeedTestUtils.createMockSeedTask(breeder);
	    replay(task);
	    replay(breeder);
	    
	    DistributedTileRangeIterator dtri = createMock(DistributedTileRangeIterator.class); {
	    	expect(dtri.nextMetaGridLocation()).andStubAnswer(new IAnswer<long[]>(){

				public long[] answer() throws Throwable {
					return tri.nextMetaGridLocation();
				}});
	    } replay(dtri);
	    
	    TileLayer tl = createMock(TileLayer.class); {
	    	expect(tl.getName()).andStubReturn("testLayer");
	    } replay(tl);
	    
	    DistributedSeedJob job = new DistributedSeedJob(1, breeder, tl, 1, dtri, false);
	    
	    job.threads[0] = task;

	    return job;
	}

	@Override
	protected Job jobWithTaskStates(STATE... states) throws Exception {

        final DistributedTileBreeder breeder = (DistributedTileBreeder) createMockTileBreeder();

        for(STATE state: states){
		    final SeedTask task = SeedTestUtils.createMockSeedTask(breeder);
		    expect(task.getState()).andStubReturn(state);
		    TaskStatus status = new TaskStatus(0, 0, 0, 0, 0, state);
		    expect(task.getStatus()).andStubReturn(status);
		    replay(task);
	    }
	    replay(breeder);
	    
	    return createTestSeedJob(breeder, states.length);
	}

	@Override
	protected TileBreeder createMockTileBreeder() {
		reset(breeder);
		expect(breeder.getHz()).andStubReturn(hzInstance);
	    return breeder;
	}

	@Override
	protected Job createTestSeedJob(TileBreeder breeder, int threads) {
	    TileLayer tl = createMock(TileLayer.class); {
	    	expect(tl.getName()).andStubReturn("testLayer");
	    } replay(tl);
	    TileRange tr = createMock(TileRange.class);
	    replay(tr);
	    DistributedTileRangeIterator tri = createMock(DistributedTileRangeIterator.class);{
	    	expect(tri.getTileRange()).andStubReturn(tr);
	    } replay(tri);
        return new DistributedSeedJob(1l, (DistributedTileBreeder) breeder, tl, threads, tri, false);
	}
}
