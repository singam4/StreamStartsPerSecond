package netflix_sps;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.FileUtils;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.util.logging.Logger;

/**
 * Stream starts per second This program consumes streaming metric data from
 * REST end point https://tweet-service.herokuapp.com/sps Agrregates the
 * successful starts per every second, grouped by (device, title, country)
 * 
 * Design : In-memory queue backed by chain of disk files As space becomes
 * available in the queue, the disk files make their way into the Queue Likewise
 * when Queue fills up, the entries spill to the chain of disk files
 * 
 * Future improvemnts based on performance evaluation: 
 * 1: Extend to range parallelism 
 * 2: Hash partitioned distribution can be evaluated (Map-Reduce) 
 * 3: SIMD and GPU parallelsim can be exploited for aggregations
 */
public class SpsDiskBacked {
	private final static Logger LOGGER = Logger.getLogger(SpsDiskBacked.class.getName());

	private static HttpURLConnection conn;
	private static File headFile;

	public static void main(String[] args) {

		final BlockingQueue<String> bq1 = new ArrayBlockingQueue<String>(2048);
		final String newLine = System.getProperty("line.separator");

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

					BufferedReader stream_br = new BufferedReader(new InputStreamReader(conn.getInputStream()));

					// This is the first file where the stream spills from queue for a session
					// rest of the spilled data is chained from this in a durable mechanism
					headFile = File.createTempFile("HEAD", ".bin");
					// headFile.deleteOnExit();
					LOGGER.info("HEAD : " + headFile.getAbsolutePath());

					// First nextfile
					File nextFile = File.createTempFile("tempfile", ".bin");
					nextFile.deleteOnExit();
					LOGGER.finer("First nextfile : " + nextFile.getAbsolutePath());
					FileUtils.writeStringToFile(headFile, nextFile.getAbsolutePath() + newLine, true);

					File currFile = headFile;
					boolean spillToDisk = false;
					int file_entry_limit = 0;

					while (true) {
						String stream_str = stream_br.readLine();
						if (stream_str.contains("busted data")
								|| !(stream_str.contains("success") || stream_str.contains("successinfo")))
							continue;

						if (!spillToDisk) { // in-memory
							if (bq1.remainingCapacity() > 1) {
								LOGGER.finest("Adding to Queue : " + stream_str.substring(6));
								while (!bq1.offer(stream_str.substring(6))) {
									LOGGER.finest("Ingestor sleeping ....");
									Thread.sleep((long) Math.random());
								}
								continue;
							} else {
								// Append to current file
								FileUtils.writeStringToFile(currFile, stream_str.substring(6) + newLine, true);
								file_entry_limit++;
								spillToDisk = true;
								LOGGER.fine("In-memory queue spilling to disk");
							}
						} else { // spilling to disk
							if (bq1.remainingCapacity() > 1024) {
								// Enough space in Queue, lets move the head file into queue
								LOGGER.finer("Moving head into Queue");

								File tmpHeadFile = headFile;
								// move head file into the queue, and make the next as current
								BufferedReader head_br = new BufferedReader(new FileReader(headFile));
								String head_str = head_br.readLine(); // First line is always nextFile
								LOGGER.finer("Embedded next file : " + head_str);

								// Reached end of spilled chain of files
								if (null == head_str) {
									spillToDisk = false;
									while (!bq1.offer(stream_str.substring(6)))
										;
									continue;
								}

								// NextFile becomes new head file
								headFile = new File(head_str);
								if (headFile.getAbsolutePath() == currFile.getAbsolutePath()) {
									currFile = headFile;
								}

								// Populate the Queue with the head file
								while ((head_str = head_br.readLine()) != null) {
									bq1.offer(head_str);
									LOGGER.finest("Moving string into Q : " + head_str);
								}
								head_br.close();
								tmpHeadFile.delete();
								LOGGER.finer("Queue size after consuming head: " + bq1.size());
							} else {
								// keep appending to current file
								if (file_entry_limit > 1024) {
									LOGGER.finer("Chaining next file");

									// create new file and chain it
									File nextNextFile = File.createTempFile("tempfile", ".bin");
									nextNextFile.deleteOnExit();

									FileUtils.writeStringToFile(nextFile, nextNextFile.getAbsolutePath() + newLine,
											true);

									currFile = nextFile;
									nextFile = nextNextFile;
									file_entry_limit = 0;
								}
							}
							FileUtils.writeStringToFile(currFile, stream_str.substring(6) + newLine, true);
							file_entry_limit++;
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
			String sCurrentLine;

			public void run() {
				try {
					Thread.sleep(10000L); // Just to excercise disk spilling

					Long prevTime = null;
					int entries_processed = 0;
					Map<String, Integer> results = new HashMap<String, Integer>();
					JSONParser parser = new JSONParser();

					while ((sCurrentLine = bq1.poll()) == null)
						;
					org.json.simple.JSONObject jsonObject = (org.json.simple.JSONObject) parser.parse(sCurrentLine);
					String device = (String) jsonObject.get("device");
					String title = (String) jsonObject.get("title");
					String country = (String) jsonObject.get("country");
					Long currTime = prevTime = (Long) jsonObject.get("time");
					results.put(device + "@" + title + "@" + country, 1);

					while (true) {
						while ((sCurrentLine = bq1.poll()) == null)
							;
						jsonObject = (org.json.simple.JSONObject) parser.parse(sCurrentLine);
						device = (String) jsonObject.get("device");
						title = (String) jsonObject.get("title");
						country = (String) jsonObject.get("country");
						currTime = (Long) jsonObject.get("time");

						if ((currTime - prevTime) > 1000) {
							// Display results in the hash map for this duration of the second
							for (Map.Entry<String, Integer> entry : results.entrySet()) {
								String key = entry.getKey();
								Integer value = entry.getValue();

								String[] att = key.split("@");
								System.out.println("{\"device\": \"" + att[0] + "\", \"sps\": " + value + ", "
										+ "\"title\": \"" + att[1] + "\", \"country\": \"" + att[2] + "\"}");
							}
							LOGGER.finest("End of batch : " + prevTime / 1000);
							prevTime = currTime;
							results.clear();

							LOGGER.finer("Entries munged : " + entries_processed);
							entries_processed = 0;
						}

						// Add to results hash map for this duration of the second
						String key = device + "@" + title + "@" + country;
						// results.merge(key, 1, Integer::sum);
						if (results.containsKey(key)) {
							results.replace(key, results.get(key) + 1);
						} else {
							results.put(key, 1);
						}
						entries_processed++;
					}

				} catch (ParseException e1) {
					LOGGER.severe("Probable broken String : " + sCurrentLine);
					e1.printStackTrace();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}

		};

		// Start the threads
		ingester.start();
		analyzer.start();
	}

}
