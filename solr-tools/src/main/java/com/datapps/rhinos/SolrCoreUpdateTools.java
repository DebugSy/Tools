package com.datapps.rhinos;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by DebugSy on 2017/11/30.
 */
public class SolrCoreUpdateTools {

	private static final Logger LOG = LoggerFactory.getLogger(SolrCoreUpdateTools.class);

	private SolrClient solrClient;

	private SolrClient client;

	private String core;

	private String url;

	public SolrCoreUpdateTools(String url) {
		solrClient = new HttpSolrClient(url);
		this.url = url;
		int lastIndexOf = url.lastIndexOf("/");
		String solrUrl = url.substring(0, lastIndexOf);
		this.core = url.substring(lastIndexOf+1);
		this.client = new HttpSolrClient(solrUrl);
	}

	public long ping() throws IOException, SolrServerException {
		SolrPingResponse pingResponse = solrClient.ping();
		long elapsedTime = pingResponse.getElapsedTime();
		return elapsedTime;
	}

	public int getCoreNumDocs() throws IOException, SolrServerException {
		CoreAdminRequest request = new CoreAdminRequest();
		request.setAction(CoreAdminParams.CoreAdminAction.STATUS);
		CoreAdminResponse response = request.process(client);
		NamedList<Object> coreStat = response.getCoreStatus().get(core);
		SimpleOrderedMap map = (SimpleOrderedMap) coreStat.get("index");
		Integer numDocs = (Integer) map.get("numDocs");
		return numDocs.intValue();
	}

	public SolrDocumentList findAllAndUpdate(int start, int rows) throws IOException, SolrServerException {
		Map<String, String> map = new HashMap<String, String>();
		map.put("q", "*:*");
		map.put("start", String.valueOf(start));
		map.put("rows", String.valueOf(rows));
		SolrParams params = new MapSolrParams(map);
		QueryResponse resp = null;
		resp = solrClient.query(params);
		SolrDocumentList docsList = resp.getResults();
		LOG.info("fetch docs size : {}", docsList.size());
		return docsList;
	}

	public void update(SolrDocumentList docsList, boolean isParentIdCheck) throws IOException, SolrServerException {
		Collection<SolrInputDocument> collection = new HashSet();
		Collection<String> ids = new HashSet<String>();
		for (SolrDocument doc : docsList) {
			SolrInputDocument solrInputDocument = ClientUtils.toSolrInputDocument(doc);
			solrInputDocument.remove("_version_");
			if (isParentIdCheck){
				ids.add((String) solrInputDocument.get("id").getValue());
			}
			collection.add(solrInputDocument);
		}

		//parentid check duplicate
		List<String> deleteIds = new ArrayList<String>();
		if (isParentIdCheck){
			Collection<SolrInputDocument> collectionCopy = new HashSet(collection);
			for (SolrInputDocument document : collectionCopy){
				Object parent = document.get("parentId");
				if (parent != null){
					Object parentId = document.get("parentId").getValue();
					String id = (String) document.get("id").getValue();
					LOG.info("parentId is {}", parentId.toString());
					if (!ids.contains(parentId)){
						LOG.warn("parentId is not exist. removed ==> id:{}", id);
						collection.remove(document);
						deleteIds.add(id);
					}
				}
			}
		}

		LOG.info("update size : {}", collection.size());
		solrClient.add(collection);
		LOG.info("delete size : {}", deleteIds.size());
		solrClient.deleteById(deleteIds);
		solrClient.commit();
	}

	public void run(){
		try {

			LOG.info("update starting......");
			int coreNumDocs = getCoreNumDocs();
			int start = 0;
			int rows = 1000;
			while (coreNumDocs > 0){
				LOG.info("coreNumDocs : {}", coreNumDocs);
				LOG.info("update sole core. start : {}, rows : {}", start, rows);
				if (coreNumDocs < rows){
					rows = coreNumDocs;
				}
				SolrDocumentList documentList = findAllAndUpdate(start, rows);
				update(documentList, true);
				start = rows;
				coreNumDocs-=rows;
			}
			LOG.info("update finished!");

		} catch (IOException e) {
			e.printStackTrace();

			try {
				solrClient.rollback();
			} catch (SolrServerException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}

		} catch (SolrServerException e) {
			e.printStackTrace();

			try {
				solrClient.rollback();
			} catch (SolrServerException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		} finally {

			try {
				solrClient.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	public static void main(String[] args) {
		if (args.length != 1){
			throw new IllegalArgumentException("arguments size is only one. please check");
		}
		String url = args[0];
		if (url == null || !url.startsWith("http://")){
			throw new IllegalArgumentException("please check argument: url. eg: http://192.168.112.128:8984/solr/rss");
		}
		SolrCoreUpdateTools solrCoreUpdateTools = new SolrCoreUpdateTools(url);
		solrCoreUpdateTools.run();
	}

}
