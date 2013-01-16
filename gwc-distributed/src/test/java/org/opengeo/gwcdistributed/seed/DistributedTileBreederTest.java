package org.opengeo.gwcdistributed.seed;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Collections;

import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.seed.AbstractJobTest;
import org.geowebcache.seed.SeedTask;
import org.geowebcache.seed.TileBreeder;
import org.geowebcache.storage.TileRange;
import org.geowebcache.storage.TileRangeIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.hazelcast.core.Hazelcast;

import static org.easymock.classextension.EasyMock.*;

public class DistributedTileBreederTest {
	static final String CONTEXT_PATH = "classpath:/org/opengeo/gwcdistributed/seed/distributedTileBreederTest-context.xml";
	static final String BREEDER_BEAN_NAME = "breeder";
	
	ApplicationContext ctxt1;
	ApplicationContext ctxt2;
	DistributedTileBreederMockedTasks breeder1;
	DistributedTileBreederMockedTasks breeder2;
	
	@Before
	public void setUp(){
		
		ctxt1=new ClassPathXmlApplicationContext(CONTEXT_PATH);
		ctxt2=new ClassPathXmlApplicationContext(CONTEXT_PATH);
		
		breeder1 = (DistributedTileBreederMockedTasks) ctxt1.getBean(BREEDER_BEAN_NAME);
		breeder2 = (DistributedTileBreederMockedTasks) ctxt2.getBean(BREEDER_BEAN_NAME);
	}

	@After
	public void tearDown(){
		Hazelcast.shutdownAll();
	}
	
	
	@Test
	public void testDistributeJob() throws Exception {
		
		Collection<SeedTask> tasks1 = Collections.singleton(createMock(SeedTask.class));
		for(SeedTask task: tasks1){
			AbstractJobTest.expectDoActionInternal(task).once();
			replay(task);
		}
		breeder1.seedIt=tasks1.iterator();
		
		Collection<SeedTask> tasks2 = Collections.singleton(createMock(SeedTask.class));
		for(SeedTask task: tasks2){
			AbstractJobTest.expectDoActionInternal(task).once();
			replay(task);
		}
		breeder2.seedIt=tasks2.iterator();
		
		TileRange tr = new TileRange(null, null, 0, 0, null, null, (String) null);
		TileRangeIterator trItr = createMock(TileRangeIterator.class);
		expect(trItr.getTileRange()).andStubReturn(tr);
		replay(trItr);
		TileLayer tl = createMock(TileLayer.class);
		expect(tl.getName()).andStubReturn("testLayer");
		replay(tl);
		
		TileLayerDispatcher tld = createMock(TileLayerDispatcher.class);
		expect(tld.getTileLayer("testLayer")).andStubReturn(tl);
		breeder1.setTileLayerDispatcher(tld);
		breeder2.setTileLayerDispatcher(tld);
		
		breeder1.createSeedJob(1, false, trItr, tl, false);

		Thread.sleep(10000);
		
		for(SeedTask task: tasks1){
			verify(task);
		}
		for(SeedTask task: tasks2){
			verify(task);
		}

	}

}
