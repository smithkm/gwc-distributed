package org.opengeo.gwcdistributed.seed;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.easymock.IAnswer;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.AbstractJobTest;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.seed.Job;
import org.geowebcache.seed.SeedTask;
import org.geowebcache.seed.SeedTestUtils;
import org.geowebcache.seed.TaskStatus;
import org.geowebcache.seed.TileBreeder;
import org.geowebcache.storage.TileRange;
import org.geowebcache.storage.TileRangeIterator;
import org.hamcrest.Matchers;
import org.hamcrest.integration.EasyMock2Adapter;

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
import com.hazelcast.core.MultiTask;

import static org.easymock.classextension.EasyMock.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.hamcrest.Matchers.*;


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
	    
	    DistributedSeedJob job = new DistributedSeedJob(1, breeder, false, tl, 1, dtri, false);
	    
	    job.threads[0] = task;

	    return job;
	}

	/**
	 * Expect a JobDistributedCallable
	 * @param job job it should affect
	 * @param clazz subclass of JobDistributedCallable it should be an instance of
	 * @return
	 */
	static <T,U extends DistributedCallable<T>> DistributedCallable<T> jdcIs(Job job, Class<U> clazz){
		EasyMock2Adapter.adapt(both(hasProperty("job", equalTo(job))).and(instanceOf(clazz)));
		return null;
	}
	
	//@Override
	protected Job jobWithTaskStates(STATE... states) throws Exception {

        final DistributedTileBreeder breeder = (DistributedTileBreeder) createMockTileBreeder();

        for(STATE state: states){
		    final SeedTask task = SeedTestUtils.createMockSeedTask(breeder);
		    expect(task.getState()).andStubReturn(state);
		    TaskStatus status = createMock(DistributedTaskStatus.class);
		    expect(status.getState()).andStubReturn(state);
		    replay(state);
		    expect(task.getStatus()).andStubReturn(status);
		    replay(task);
	    }
	    replay(breeder);
	    
	    final Job job =  createTestSeedJob(breeder, states.length);
	    
	    // TODO, separate out setup for termination tests from just setting up state.
	    final MultiTask<Object> mtask = createMock(MultiTask.class);
	    expect(mtask.get()).andReturn(null).times(0, 1);
	    replay(mtask);
	    verify(breeder);
	    reset(breeder);
	    expect(breeder.executeCallable(jdcIs(job, DoTerminateJob.class))).andAnswer(new IAnswer<MultiTask<Object>>(){

			public MultiTask<Object> answer() throws Throwable {
				((DistributedJob)job).terminateLocal();
				return mtask;
			}}).times(0, 1);
	    replay(breeder);
	    
	    return job;
	}

	@Override
	protected TileBreeder createMockTileBreeder() {
		reset(breeder);
		expect(breeder.getHz()).andStubReturn(hzInstance);
	    return breeder;
	}

	//@Override
	protected Job createTestSeedJob(TileBreeder breeder, int threads) {
	    TileLayer tl = createMock(TileLayer.class); {
	    	expect(tl.getName()).andStubReturn("testLayer");
	    } replay(tl);
	    TileRange tr = createMock(TileRange.class);
	    replay(tr);
	    DistributedTileRangeIterator tri = createMock(DistributedTileRangeIterator.class);{
	    	expect(tri.getTileRange()).andStubReturn(tr);
	    } replay(tri);
        return new DistributedSeedJob(1l, (DistributedTileBreeder) breeder, false, tl, threads, tri, false);
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
