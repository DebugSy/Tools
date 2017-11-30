package com.datapps.rhinos;

import org.apache.solr.client.solrj.SolrServerException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by DebugSy on 2017/11/30.
 */
public class SolrCoreUpdateToolsTest {

	private SolrCoreUpdateTools solrCoreUpdateTools;

	private String coreUrl = "http://192.168.112.128:8984/solr/rss_reindex";

	@Before
	public void init(){
		solrCoreUpdateTools = new SolrCoreUpdateTools(coreUrl);
	}

	@Test
	public void testGetCoreSize() throws IOException, SolrServerException {
		int size = solrCoreUpdateTools.getCoreNumDocs();
		System.out.println(size);
	}

	@Test
	public void testPingSolr() throws IOException, SolrServerException {
		long ping = solrCoreUpdateTools.ping();
		System.out.println("ping time: " + ping);
	}

	@Test
	public void testFindAll() throws IOException, SolrServerException {
		solrCoreUpdateTools.findAllAndUpdate(1, 10);
	}

	@Test
	public void testRun(){
		solrCoreUpdateTools.run();
	}

}
