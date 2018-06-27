package netflix_sps;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.json.simple.parser.JSONParser;
import java.io.FileReader;

public class StreamStartsPerSecond {

	// TODO 1: We can design with an in memory queue for higher performance gains, with compromise on durability
	// TODO 2: Extendable to range parallelism 
	// TODO 3: Hash partitioned distribution can be evaluated too
	// TODO 4: SIMD and GPU parallelsim can be exploited for aggregations
	
	private static HttpURLConnection conn;
	private static File head;

	public static void main(String[] args) {

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

					// This is the first file where the stream is written for a session
					// rest of the data is chained from this in a durable mechanism 
					head = File.createTempFile("HEAD", ".bin");
					System.out.println("HEAD : " + head.getAbsolutePath());
					File currFile = head;

					int entry_limit = 0;
					while (true) {
						String output;
						BufferedWriter bw = new BufferedWriter(new FileWriter(currFile));

						// Limit entries to 1K lines per file
						while ((output = br.readLine()) != null && entry_limit++ <= 1024) {
							if (output.equals(""))
								continue;
							bw.write(output);
							bw.newLine();
						}
						bw.close();
						entry_limit = 0;

						File nextFile = File.createTempFile("tempfile", ".bin");
						nextFile.deleteOnExit();

						// Append the next file location at the end of current file
						FileUtils.writeStringToFile(currFile, "\nNEXTFILE : " + nextFile.getAbsolutePath(), true);
						currFile = nextFile;
					}

				} catch (MalformedURLException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					conn.disconnect();
				}
			}
		};

		// Analyzer Thread
		Thread analyzer = new Thread() {
			BufferedReader br;

			public void run() {
				try {
					File currFile = head;
					String sCurrentLine;
					Long prevTime = null;
					Map<String, Integer> results = new HashMap<String, Integer>();
					JSONParser parser = new JSONParser();					
					
					while (null == head || !head.canRead()) {
						// spin until head file is created
						long sleepTime = (long) (Math.random() * 1000);
						System.out.println("Analyzer thread sleeping for : " + sleepTime);
						Thread.sleep(sleepTime);
					}
					br = new BufferedReader(new FileReader(head));
					Thread.sleep((long) (Math.random() * 1000)); // Give time for buffer to fill

					while(br.ready()) {
						sCurrentLine = br.readLine();
						if (sCurrentLine.equals("") || sCurrentLine.contains("busted data"))
							continue;
						if (sCurrentLine.startsWith("NEXTFILE")) {
							// TODO: Can free current file and instantiate the next file as current					
							currFile = new File(sCurrentLine.substring(11));

							int retries = 0;
							while (!currFile.canRead() && ++retries<100) {
								// spin until next file is complete
								long sleepTime = (long) (Math.random() * 100);
								System.out.println("Analyzer thread sleeping for : " + sleepTime);
								Thread.sleep(sleepTime);
							}
							
							// Bail out if retries reached 100
							if(retries == 100) {
								System.out.println("******Big RED button********");
								System.exit(0);
							}
							
							//System.out.println("Switching to next file : " + nextFile);
							br = new BufferedReader(new FileReader(currFile));
							Thread.sleep((long) (Math.random() * 100)); // Give time for buffer to fill
							continue;
						}
						String jsonString = sCurrentLine.substring(6);

						try {
							org.json.simple.JSONObject jsonObject = (org.json.simple.JSONObject) parser.parse(jsonString);
							String device = (String) jsonObject.get("device");
							String sev = (String) jsonObject.get("sev");
							String title = (String) jsonObject.get("title");
							String country = (String) jsonObject.get("country");
							Long currTime = (Long) jsonObject.get("time");

							if (sev.equalsIgnoreCase("success") || sev.equalsIgnoreCase("successinfo")) {
								if (prevTime == null) {
									prevTime = currTime;
								} else if ((currTime-prevTime) > 1000) {
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
								} else if (currTime-prevTime <= 1000) {
									// Add to results hash map for this duration of the second
									String key = device + "@" + title + "@" + country;
									if (results.containsKey(key)) {
										results.replace(key, results.get(key) + 1);
									} else {
										results.put(key, 1);
									}
								}
							}

						} catch (Exception e) {
							System.out.println("Probable broken String : " + jsonString + "\n in file : " + currFile);
							e.printStackTrace();
						}
					}

				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					try {
						if (br != null)
							br.close();
					} catch (IOException ex) {
						ex.printStackTrace();
					}
				}
			}

		};

		// Start the threads
		ingester.start();
		analyzer.start();
	}

}