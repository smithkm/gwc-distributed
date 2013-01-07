package org.opengeo.gwcdistributed.seed;

import static org.junit.Assert.*;

import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.AbstractJobTest;
import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.seed.Job;
import org.geowebcache.seed.TruncateTask;
import org.geowebcache.storage.TileRangeIterator;
import org.junit.Test;
import static org.easymock.classextension.EasyMock.*;

public class DistributedTruncateJobTest extends AbstractJobTest {

	@Override
	protected Job initNextLocation(TileRangeIterator tri) {
	    final DistributedTileBreeder breeder = createMock(DistributedTileBreeder.class);
	    final TruncateTask task = createMockTruncateTask(breeder);
	    replay(task);
	    replay(breeder);
	    
	    TileLayer tl = createMock(TileLayer.class);
	    replay(tl);
	    
	    DistributedTruncateJob job = new DistributedTruncateJob(1, breeder, tl, tri, false);
	    
	    job.threads[0] = task;

	    return job;
	}

	@Override
	protected Job jobWithTaskStates(STATE... states) {
	    final DistributedTileBreeder breeder = createMock(DistributedTileBreeder.class);
	    final TruncateTask task = createMockTruncateTask(breeder);
	    expect(task.getState()).andReturn(states[0]).anyTimes();
	    replay(task);
	    replay(breeder);
	    
	    TileLayer tl = createMock(TileLayer.class);
	    replay(tl);
	    TileRangeIterator tri = createMock(TileRangeIterator.class);
	    replay(tri);
	    
	    DistributedTruncateJob job = new DistributedTruncateJob(1, breeder, tl, tri, false);
	    
	    return job;
	}
	
	@Override
	protected void assertGetState(STATE expected, STATE... states) {
	    if(states.length<=1){
	        super.assertGetState(expected, states);
	    } else {
	        // Don't test as this job can never have more than one task.
	    }
	}

}
