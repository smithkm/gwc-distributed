package org.opengeo.gwcdistributed.seed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.seed.GWCTask;
import org.geowebcache.seed.GWCTask.STATE;
import org.geowebcache.seed.Job;
import org.geowebcache.seed.JobNotFoundException;
import org.geowebcache.seed.JobStatus;
import org.geowebcache.seed.JobUtils;
import org.geowebcache.seed.TaskStatus;
import org.geowebcache.seed.TileBreeder;
import org.geowebcache.seed.TileRequest;
import org.geowebcache.seed.threaded.ThreadedTileBreeder;
import org.geowebcache.storage.TileRange;
import org.geowebcache.storage.TileRangeIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiTask;
import com.hazelcast.spring.context.SpringAware;

@SpringAware
public abstract class DistributedJob implements Job, Serializable {

    final private AtomicNumber activeThreads;
    final protected int threadCount;
    final protected long id;
    final protected DistributedTileRangeIterator trItr;
    final protected String layerName;
    
    public static final long TIME_NOT_STARTED=-1;
    private long groupStartTime=TIME_NOT_STARTED;
    
    private final boolean doFilterUpdate;
    

    transient protected TileLayer tl;
    transient protected GWCTask[] threads;
    transient protected DistributedTileBreeder breeder;

    
    public static Log log = LogFactory.getLog(DistributedJob.class);

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
            DistributedTileRangeIterator tri, boolean doFilterUpdate) {

        Assert.notNull(breeder);
        Assert.notNull(tl);
        Assert.notNull(tri);
        Assert.isTrue(threadCount>0,"threadCount must be positive");
        Assert.isTrue(id>=0,"Job id must be non-negative");
        
        this.breeder = breeder;
        this.threadCount = threadCount;
        this.id = id;
        final AtomicNumber iteratorStep = breeder.getHz().getAtomicNumber(this.getKey("iteratorStep"));
        this.trItr = tri;
        this.tl = tl;
        this.layerName = tl.getName();
        this.doFilterUpdate = doFilterUpdate;
        com.hazelcast.core.HazelcastInstance hz = breeder.getHz();
        this.activeThreads = hz.getAtomicNumber(getKey("activeThreadCount"));
        
        createTasks();
    }

    /**
     * Create the tasks for this job.
     */
    protected abstract void createTasks();
    
    /**
     * Get a key unique to this Job but shared by all instances of it across the cluster.
     * @param baseKey 
     * @return A string incorporating the base key and the unique identifier for this job.
     */
    protected String getKey(String baseKey){
    	return getKey(baseKey, id);
    }
    
    static String getKey(String baseKey, long id){
    	if(baseKey!=null)
    		return String.format("job-%d-%s", id, baseKey);
    	else
    		return String.format("job-%d", id);    	
    }
	
	public long getId() {
		return id;
	}
	@SpringAware
	static class DistributedTileRequest implements TileRequest, Serializable {
		
		final long[] gridLoc;
		final long jobId;
		long retryAt = 0;
		long failures = 0;
		
		transient Job job;
		
		

		public DistributedTileRequest(long[] gridLoc, Job job) {
			super();
			this.gridLoc = gridLoc;
			this.job = job;
			this.jobId = job.getId();
		}

		public long[] getGridLoc() {
			return gridLoc;
		}

		public long getX() {
			return gridLoc[0];
		}

		public long getY() {
			return gridLoc[1];
		}

		public long getZoom() {
			return gridLoc[2];
		}

		public long getRetryAt() {
			return retryAt;
		}

		public long getFailures() {
			return failures;
		}

		public Job getJob() {
			return job;
		}

		public int compareTo(TileRequest o) {
			return Long.signum(getRetryAt()-o.getRetryAt());
		}
		
		@Autowired
		public void setBreeder(DistributedTileBreeder breeder) throws GeoWebCacheException{
			this.job = breeder.getJobByID(jobId);
		}
	}
	
	public TileRequest getNextLocation() throws InterruptedException {
		long[] gridLoc = trItr.nextMetaGridLocation();
		if(gridLoc==null) return null;
		TileRequest tr = new DistributedTileRequest(gridLoc, this);
		return tr;
	}

	public GWCTask[] getTasks() {
		return threads;
	}
	
	/**
	 * Get the status of each task in the job, across the cluster.
	 * @return
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public Collection<TaskStatus> getClusterTasksStatus() throws ExecutionException, InterruptedException {
    	assertInitialized();
		final Set<Member> members = breeder.getHz().getCluster().getMembers();
		final MultiTask<Collection<TaskStatus>> mtask = new MultiTask<Collection<TaskStatus>>(new GetTaskStatus(this.getId()), members);
		breeder.getHz().getExecutorService().submit(mtask);
		Collection<Collection<TaskStatus>> statusTree = mtask.get();
		
		// Flatten into single collection
		List<TaskStatus> result = new ArrayList<TaskStatus>((int)(statusTree.size()*this.getThreadCount()));
		for(Collection<TaskStatus> statusForNode: mtask.get()) {
			result.addAll(statusForNode);
		}
		
		return result;
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
    	assertInitialized();
		return breeder;
	}

	public TileLayer getLayer() {
		return tl;
	}

	public TileRange getRange() {
		return trItr.getTileRange();
	}

    public JobStatus getStatus() {
    	assertInitialized();
        Collection<TaskStatus> taskStatuses = new ArrayList<TaskStatus>(threads.length);
        for(GWCTask task: threads) {
            taskStatuses.add(task.getStatus());
        }
        return new JobStatus(taskStatuses, System.currentTimeMillis(), this.id);
    }

    class StateIterator implements Iterator<GWCTask.STATE> {
    	
    	Iterator<TaskStatus> it = getStatus().getTaskStatuses().iterator();
    	
		public boolean hasNext() {
			return it.hasNext();
		}

		public STATE next() {
			return it.next().getState();
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
    	
    }
    
	public STATE getState() {
		return JobUtils.combineState(new StateIterator());
	}
	
	/**
	 * Assert that a task belongs to this job
	 * @param task
	 */
    protected void myTask(GWCTask task) {
        Assert.isTrue(task.getJob()==this, "Task does not belong to this Job");
    }

    /**
     * Check that transient fields have been initialized after being deserialized.
     */
    protected void assertInitialized() {
    	Assert.state(this.breeder!=null || this.threads!=null, "Local state was not correctly set after being deserialized.");
    }
    
    /**
     * Property setter for Spring, should only be called once.
     * @param breeder
     * @throws GeoWebCacheException 
     */
    @Autowired
    public void setBreeder(final DistributedTileBreeder breeder) throws GeoWebCacheException {
    	Assert.state(this.breeder==null, "Breeder should only be set once by Spring");
    	Assert.notNull(breeder);
    	
    	this.breeder = breeder;
    	
    	// Set up other transient fields from breeder.
    	this.tl = breeder.getTileLayerDispatcher().getTileLayer(layerName);
    	try{
    		this.threads = ((DistributedJob)breeder.getJobByID(this.id)).threads;
    		log.info("Local job found, acquired local tasks.");
    	} catch (JobNotFoundException ex) {
    		log.info("Local job not found, creating new set of local tasks.");
    		createTasks();
    	}

    }
    
}
