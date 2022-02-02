package in.pazmysaz.essql;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask.Status;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class BatchUpdateWithPainlesScriptingTest {
	private static final org.slf4j.Logger logger = LoggerFactory.getLogger("Batch-update-test");

	private static final Script script = new Script(ScriptType.INLINE, "painless",
			"ctx._source.spend=ctx._source.spend+100000;ctx._source.like=ctx._source.like+20000",
			Collections.emptyMap());

	@Test
	public void testTwoColumnUpdate() {
		RestHighLevelClient client = new RestHighLevelClient( /* Changed from 9201 to 9300 by DS */
				RestClient.builder(new HttpHost("casey.store", 9200, "http"),
						new HttpHost("casey.store", 9300, "http")));
		
		UpdateByQueryRequest request = new UpdateByQueryRequest("schema_space_pimkpi");
		request.setQuery(new MatchQueryBuilder("_id", "223031757439781803"));
		request.setScript(script);
		request.setRefresh(true);
		
	
		
		try {
			BulkByScrollResponse bulkResponse = client.updateByQuery(request, RequestOptions.DEFAULT);
			int batches = bulkResponse.getBatches();
			List<Failure> bulkFailures = bulkResponse.getBulkFailures();
			long bulkRetries = bulkResponse.getBulkRetries();
			long created = bulkResponse.getCreated();
			long noops = bulkResponse.getNoops();
			String reasonCancelled = bulkResponse.getReasonCancelled();
			long searchRetries = bulkResponse.getSearchRetries();
			Status status = bulkResponse.getStatus();
			TimeValue took = bulkResponse.getTook();
			long total = bulkResponse.getTotal();
			long updated = bulkResponse.getUpdated();
			long versionConflicts = bulkResponse.getVersionConflicts();

			System.out.println("Batches = " + batches);
			System.out.println("BulkFailures = " + bulkFailures);
			System.out.println("BulkRetries = " + bulkRetries);
			System.out.println("Created = " + created);
			System.out.println("Noops = " + noops);

			System.out.println("ReasonCancelled = " + reasonCancelled);
			System.out.println("SearchRetries = " + searchRetries);
			System.out.println("Status = " + status);
			System.out.println("Timetook = " + took);

			System.out.println("Total = " + total);
			System.out.println("Updated = " + updated);
			System.out.println("VersionConflicts = " + versionConflicts);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	public void testMultiRowUpdate() {
		RestHighLevelClient client = new RestHighLevelClient( /* Changed from 9201 to 9300 by DS */
				RestClient.builder(new HttpHost("casey.store", 9200, "http"),
						new HttpHost("casey.store", 9300, "http")));

		UpdateRequest request = new UpdateRequest("schema_space_pimkpi", "223031757439781803");
		request.script(script);

		UpdateRequest request1 = new UpdateRequest("schema_space_pimkpi", "6931072944882664691");
		request1.script(script);

		BulkRequest rbrequest = new BulkRequest("schema_space_pimkpi");
		rbrequest.add(request);
		rbrequest.add(request1);
		rbrequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
		rbrequest.setRefreshPolicy("wait_for");
		request.waitForActiveShards(2);

		try {
			BulkResponse bulkResponse = client.bulk(rbrequest, RequestOptions.DEFAULT);
			String buildFailureMessage = bulkResponse.buildFailureMessage();
			TimeValue ingestTook = bulkResponse.getIngestTook();
			TimeValue took = bulkResponse.getTook();
			boolean hasFailures = bulkResponse.hasFailures();
			RestStatus status = bulkResponse.status();

			System.out.println("Batches = " + buildFailureMessage);
			System.out.println("IngestTook = " + ingestTook);
			System.out.println("Took = " + took);
			System.out.println("HasFailures = " + hasFailures);
			System.out.println("Status = " + status);
		} catch (IOException e) { // TODO Auto-generated catch block

			e.printStackTrace();
		}

	}

	@Test
	public void testMultiRowUpdateAsync() {
		RestHighLevelClient client = new RestHighLevelClient( /* Changed from 9201 to 9300 by DS */
				RestClient.builder(new HttpHost("casey.store", 9200, "http"),
						new HttpHost("casey.store", 9300, "http")));

		UpdateRequest request = new UpdateRequest("schema_space_pimkpi", "223031757439781803");
		request.script(script);

		UpdateRequest request1 = new UpdateRequest("schema_space_pimkpi", "6931072944882664691");
		request1.script(script);

		BulkRequest rbrequest = new BulkRequest("schema_space_pimkpi");
		rbrequest.add(request);
		rbrequest.add(request1);
		rbrequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
		rbrequest.setRefreshPolicy("wait_for");
		rbrequest.waitForActiveShards(2);

		ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
			@Override
			public void onResponse(BulkResponse bulkResponse) {
				String buildFailureMessage = bulkResponse.buildFailureMessage();
				TimeValue ingestTook = bulkResponse.getIngestTook();
				TimeValue took = bulkResponse.getTook();
				boolean hasFailures = bulkResponse.hasFailures();
				RestStatus status = bulkResponse.status();

				System.out.println("Batches = " + buildFailureMessage);
				System.out.println("IngestTook = " + ingestTook);
				System.out.println("Took = " + took);
				System.out.println("HasFailures = " + hasFailures);
				System.out.println("Status = " + status);
			}

			@Override
			public void onFailure(Exception e) {
				e.printStackTrace();

			}
		};

		Cancellable cancellable = client.bulkAsync(rbrequest, RequestOptions.DEFAULT, listener);
		cancellable.hashCode();
		
	}
	
	@Test
	public void testUpdateWithTermQuery()
	{
		
	}
	
}
