package netflix_sps;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.json.simple.parser.JSONParser;

public class SpsInMemory {

	private static HttpURLConnection conn;

	public static void main(String[] args) {
		final Queue<String> mq1 = new ArrayBlockingQueue<String>(1024);

		// Ingester thread
		Thread ingester = new Thread() {
			public void run() {
				try {
					URL url = new URL("https://tweet-service.herokuapp.com/sps");
					conn = (HttpURLConnection) url.openConnection();
					conn.setRequestMethod("GET");
					conn.setRequestProperty("Accept", "application/json");

					if (conn.getResponseCode() != 200) {
						throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
					}

					BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));

					while (true) {
						String output;

						while ((output = br.readLine()) != null) {
							if (output.isEmpty() || output.equals("") || output.contains("busted data"))
								continue;
							if (output.contains("success") || output.contains("successinfo")) {
								//System.out.println("Adding to Q : " + output.substring(6));
								while(!mq1.offer(output.substring(6))) {
									//System.out.println("Ingestor sleeping ....");
									Thread.sleep((long) Math.random());
								}
							}
						}
					}

				} catch (MalformedURLException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					conn.disconnect();
				}
			}
		};

		// Analyzer Thread
		Thread analyzer = new Thread() {

			public void run() {
				try {
					String jsonString;
					Long prevTime = null;
					Map<String, Integer> results = new HashMap<String, Integer>();
					JSONParser parser = new JSONParser();

					while (true) {
						while ((jsonString = mq1.poll()) == null) {
							//System.out.println("Analyzer sleeping ...");
							Thread.sleep((long) Math.random());
						}

						//System.out.println("Dequeued : " + jsonString);
						org.json.simple.JSONObject jsonObject = (org.json.simple.JSONObject) parser.parse(jsonString);
						String device = (String) jsonObject.get("device");
						String title = (String) jsonObject.get("title");
						String country = (String) jsonObject.get("country");
						Long currTime = (Long) jsonObject.get("time");

						if (prevTime == null) {
							prevTime = currTime;
						} else if ((currTime - prevTime) > 1000) {
							// Display results in the hash map for this duration of the second
							for (Map.Entry<String, Integer> entry : results.entrySet()) {
								String key = entry.getKey();
								Integer value = entry.getValue();

								String[] att = key.split("@");
								System.out.println("{\"device\": \"" + att[0] + "\", \"sps\": " + value + ", "
										+ "\"title\": \"" + att[1] + "\", \"country\": \"" + att[2] + "\"}");
							}
							prevTime = currTime;
							results.clear();
						}

						// Add to results hash map for this duration of the second
						String key = device + "@" + title + "@" + country;
						if (results.containsKey(key)) {
							results.replace(key, results.get(key) + 1);
						} else {
							results.put(key, 1);
						}
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		};

		// Start the threads
		ingester.start();
		analyzer.start();
	}

}
