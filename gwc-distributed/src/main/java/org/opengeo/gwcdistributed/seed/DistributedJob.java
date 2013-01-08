package org.opengeo.gwcdistributed.seed;

import java.util.ArrayList;
import java.util.Collection;

import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.seed.Job;
import org.geowebcache.seed.JobStatus;
import org.geowebcache.seed.TaskStatus;
import org.geowebcache.seed.TileBreeder;
import org.geowebcache.seed.TileRequest;
import org.geowebcache.storage.TileRange;
import org.geowebcache.storage.TileRangeIterator;
import org.springframework.util.Assert;

import com.hazelcast.core.AtomicNumber;

public abstract class DistributedJob implements Job {

    final private AtomicNumber activeThreads;
    final protected DistributedTileBreeder breeder;
    final protected int threadCount;
    final protected long id;
    final protected TileRangeIterator tri;
    final protected TileLayer tl;
    
    public static final long TIME_NOT_STARTED=-1;
    private long groupStartTime=TIME_NOT_STARTED;
    
    private final boolean doFilterUpdate;
    
    protected GWCTask[] threads;
  
    /**
     * 
     * @param id unique ID of the Job
     * @param breeder the TileBreeder that created this job
     * @param tl the layer this job is operating on
     * @param threadCount the number of threads to try to use
     * @param tri iterator over the tiles to be handled
     * @param doFilterUpdate update relevant filters on the layer after the job completes
     */
    protected DistributedJob(long id, DistributedTileBreeder breeder, TileLayer tl, int threadCount, 
            TileRangeIterator tri, boolean doFilterUpdate) {

        Assert.notNull(breeder);
        Assert.notNull(tl);
        Assert.notNull(tri);
        Assert.isTrue(threadCount>0,"threadCount must be positive");
        Assert.isTrue(id>=0,"Job id must be non-negative");
        
        this.breeder = breeder;
        this.threadCount = threadCount;
        this.id = id;
        this.tri = tri;
        this.tl = tl;
        this.doFilterUpdate = doFilterUpdate;
        com.hazelcast.core.HazelcastInstance hz = breeder.getHz();
        this.activeThreads = hz.getAtomicNumber(getKey("activeThreadCount"));
    }

    /**
     * Get a key unique to this Job but shared by all instances of it across the cluster.
     * @param baseKey 
     * @return A string incorporating the base key and the unique identifier for this job.
     */
    protected String getKey(String baseKey){
    	if(baseKey!=null)
    		return String.format("job-%d-%s", id, baseKey);
    	else
    		return String.format("job-%d", id);
    }
	
	public long getId() {
		return id;
	}

	public TileRequest getNextLocation() throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	public GWCTask[] getTasks() {
		return threads;
	}

	public void terminate() {
		// TODO Auto-generated method stub

	}

	public long getThreadCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void threadStarted(GWCTask thread) {
		// TODO Auto-generated method stub

	}

	public void threadStopped(GWCTask thread) {
		// TODO Auto-generated method stub

	}

	public TileBreeder getBreeder() {
		return breeder;
	}

	public TileLayer getLayer() {
		return tl;
	}

	public TileRange getRange() {
		return tri.getTileRange();
	}

    public JobStatus getStatus() {
        Collection<TaskStatus> taskStatuses = new ArrayList<TaskStatus>(threads.length);
        for(GWCTask task: threads) {
            taskStatuses.add(task.getStatus());
        }
        return new JobStatus(taskStatuses, System.currentTimeMillis(), this.id);
    }

	public STATE getState() {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * Assert that a task belongs to this job
	 * @param task
	 */
    protected void myTask(GWCTask task) {
        Assert.isTrue(task.getJob()==this, "Task does not belong to this Job");
    }

}
