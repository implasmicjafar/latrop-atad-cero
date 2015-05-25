/*
 The Software shall be used for Good, not Evil.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.
 */
package com.technojafar.orecdatagrabber;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.json.JSONArray;

/**
 *
 * @author Jafaru Mohammed
 * @version 1.0 2015-05-24 Class to model acquisition from Irradiance Sites
 */
public class IrradianceModelSite extends ModelSite {

    private int _id_u = 0;
    private String _description = "";
    private String _location = "";
    private String _url = "";
    private int _id = 0;
    private String _unit = "";

    public IrradianceModelSite(String tablePrefix, int id_u, String description, String location, String url, int id, String unit, DB db) {
        super(tablePrefix, "", description, location, url, id, unit, db);
        _id_u = id_u;
        _description = description;
        _location = location;
        _url = url;
        _id = id;
        _unit = unit;
    }

    @Override
    public void run() {
        if (_running) {
            try {
                // Action here 

                // Retieve the last timestamp logged
                String latestTimestamp = _startTime;
                DBCollection col = _db.getCollection(GetCollectionName("irradiance_data"));
                try (DBCursor cur = col.find(new BasicDBObject("irradiance_id", _id_u)).sort(new BasicDBObject("timestamp", 1)).limit(1)) {
                    if (cur.hasNext()) {
                        latestTimestamp = (String) cur.next().get("timestamp");
                    }
                    cur.close();
                }
                // Connect to the web server - Always redirect
                CloseableHttpClient httpclient = HttpClientBuilder.create()
                        .setDefaultCookieStore(new BasicCookieStore())
                        .setRedirectStrategy(new LaxRedirectStrategy())
                        .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.BROWSER_COMPATIBILITY).build())
                        .build();

                // Set headers
                CloseableHttpResponse response = null;
                HttpGet httpget = new HttpGet("https://www.auroravision.net");
                httpget.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.66 Safari/537.36");
                httpget.addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
                httpget.addHeader("Accept-Language", "en-US,en;q=0.8");
                httpget.addHeader("Connection", "Keep-Alive");
                try {
                    // Don't do anything if the time in the database is not 10 minutes from now
                    Calendar ca = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                    Calendar ca0 = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                    SimpleDateFormat formati = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    ca0.setTime(formati.parse(latestTimestamp));
                    if ((ca.getTimeInMillis() - ca0.getTimeInMillis()) < (10 * 60 * 1000)) {
                        return;
                    }
                    SimpleDateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    long startDate = format.parse(latestTimestamp).getTime() + 1;
                    Calendar target = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                    long today = target.getTimeInMillis();
                    // Give 10 minutes away
                    today = today - (10 * 60 * 1000);
                    // Don't get more than 5 days worth of data at once
                    long tempStart = startDate + (86400000 * 5);
                    // Don't jump today
                    if (tempStart > today) {
                        tempStart = today;
                    }
                    {
                        col = _db.getCollection(GetCollectionName("irradiance_data"));
                        BulkWriteOperation mongoBuilder = col.initializeOrderedBulkOperation();
                        int dbCount = 0;
                        while (tempStart < today) {
                            long fT = 0;
                            long StaTarg = new Double(startDate / 1000.0).longValue();
                            long StoTarg = new Double(tempStart / 1000.0).longValue();
                            String loc = _url + _systemId + "/" + _id + "/" + StaTarg + "/" + StoTarg;
                            httpget.setURI(new URI(loc));
                            response = httpclient.execute(httpget);
                            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                                BufferedReader rd = new BufferedReader(
                                        new InputStreamReader(response.getEntity().getContent()));
                                StringBuilder result = new StringBuilder();
                                String line = "";
                                while ((line = rd.readLine()) != null) {
                                    result.append(line);
                                }
                                response.close();
                                response = null;

                                JSONArray entries = new JSONArray(result.toString());
                                for (int i = 0; i < entries.length(); i++) {
                                    JSONArray entry = entries.getJSONArray(i);

                                    DBObject testItem = new BasicDBObject("irradiance_id", _id_u).append("timestamp", entry.getString(0));
                                    boolean found = false;
                                    try (DBCursor cur = col.find(testItem)) {
                                        if (cur.hasNext()) {
                                            found = true;
                                        }
                                        cur.close();
                                    }
                                    if (!found) {
                                        testItem.put("irradiance", entry.getDouble(1));
                                        mongoBuilder.insert(testItem);
                                        dbCount++;
                                    }
                                    fT = format.parse(entry.getString(0)).getTime();
                                    dbCount++;
                                }
                            } else {
                                return;
                            }
                            if (dbCount >= 100) {
                                mongoBuilder.execute();
                                mongoBuilder = col.initializeOrderedBulkOperation();
                                dbCount = 0;
                            }
                            if (fT != 0) {
                                startDate = fT + 1;
                            } else {
                                startDate = tempStart + 1;
                            }
                            tempStart = startDate + 86400000;
                        }
                        if (dbCount > 0) {
                            mongoBuilder.execute();
                        }

                    }
                } catch (IOException | URISyntaxException | ParseException ex) {
                    Logger.getLogger(IrradianceModelSite.class.getName()).log(Level.SEVERE, null, ex);
                } finally {
                    try {
                        if (response != null) {
                            response.close();
                        }
                        httpclient.close();
                    } catch (IOException ex1) {
                        Logger.getLogger(IrradianceModelSite.class.getName()).log(Level.SEVERE, null, ex1);
                    }
                }
            } finally {
                Stop();
            }
        }
    }
}
