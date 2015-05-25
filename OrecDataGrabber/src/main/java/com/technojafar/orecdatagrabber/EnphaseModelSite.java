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
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author Jafaru Mohammed
 * @version 1.0 2015-05-20 This class is intended to handle the extraction of
 * Data from enphase sites.
 */
public class EnphaseModelSite extends ModelSite {

    public EnphaseModelSite(String tablePrefix, String systemId, String systemName, String username, String password, int farmId, String apiKey, DB db) {
        super(tablePrefix, systemId, systemName, username, password, farmId, apiKey, db);
    }

    @Override
    public void run() {
        try {
            // Download enphase data
            if (_running) {
                try {
                    // Retieve the last timestamp logged
                    String latestTimestamp = _startTime;
                    DBCollection col = _db.getCollection(GetCollectionName("inverter_data"));
                    try (DBCursor cur = col.find(new BasicDBObject("farmid", _farmId)).sort(new BasicDBObject("timestamp", 1)).limit(1)) {
                        if (cur.hasNext()) {
                            latestTimestamp = (String) cur.next().get("timestamp");
                        }
                        cur.close();
                    }

                    // Don't do anything if the time in the database is not 10 minutes from now
                    Calendar ca = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                    Calendar ca0 = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                    SimpleDateFormat formati = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    ca0.setTime(formati.parse(latestTimestamp));
                    if ((ca.getTimeInMillis() - ca0.getTimeInMillis()) < (10 * 60 * 1000)) {
                        return;
                    }

                    // Connect to the web server - Always redirect
                    CloseableHttpClient httpclient = HttpClientBuilder.create().setRedirectStrategy(new LaxRedirectStrategy()).build();

                    // Set headers
                    CloseableHttpResponse response = null;
                    HttpGet httpget = new HttpGet("https://enlighten.enphaseenergy.com/login");
                    httpget.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.66 Safari/537.36");
                    httpget.addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
                    httpget.addHeader("Accept-Language", "en-US,en;q=0.8");
                    httpget.addHeader("Connection", "Keep-Alive");

                    try {
                        response = httpclient.execute(httpget);

                        // We require good response
                        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                            BufferedReader rd = new BufferedReader(
                                    new InputStreamReader(response.getEntity().getContent()));
                            StringBuffer result = new StringBuffer();
                            String line = "";
                            while ((line = rd.readLine()) != null) {
                                result.append(line);
                            }
                            // Close the response
                            response.close();
                            response = null;
                            // Load the web page
                            Document doc = Jsoup.parse(result.toString());
                            // Get input fields
                            Elements elements = doc.getElementsByTag("input");
                            Map<String, String> post_data = new HashMap<>();
                            for (int i = 0; i < elements.size(); i++) {
                                // Add the input to the post
                                Element element = elements.get(i);
                                String element_name = element.attr("name");
                                if (element_name != null) {
                                    if ("user[email]".equals(element_name.toLowerCase())) {
                                        post_data.put(element_name, _username);
                                    } else if ("user[password]".equals(element_name.toLowerCase())) {
                                        post_data.put(element_name, _password);
                                    } else {
                                        post_data.put(element_name, element.attr("value"));
                                    }
                                }
                            }
                            // Build the query
                            RequestBuilder builder = RequestBuilder.post().setUri(new URI("https://enlighten.enphaseenergy.com/login/login"));
                            Iterator it = post_data.entrySet().iterator();
                            while (it.hasNext()) {
                                Map.Entry pairs = (Map.Entry) it.next();
                                builder.addParameter((String) pairs.getKey(), (String) pairs.getValue());
                                it.remove(); // avoids a ConcurrentModificationException
                            }
                            HttpUriRequest login = builder.build();
                            HttpContext context = new BasicHttpContext();
                            response = httpclient.execute(login, context);
                            HttpUriRequest currentReq = (HttpUriRequest) context.getAttribute(
                                    ExecutionContext.HTTP_REQUEST);
                            HttpHost currentHost = (HttpHost) context.getAttribute(
                                    ExecutionContext.HTTP_TARGET_HOST);
                            String lastLocation = (currentReq.getURI().isAbsolute()) ? currentReq.getURI().toString() : (currentHost.toURI() + currentReq.getURI());
                            Header[] h = response.getAllHeaders();
                            boolean success = false;
                            for (Header hea : h) {
                                if (hea.getName().equals("ETag")) {
                                    success = true;
                                }
                                httpget.addHeader(hea);
                            }
                            // Response must be good;
                            if (success && (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK)) {
                                // Verify login was successfull
                                success = false;
                                String[] pieces = null;
                                String[] rpieces = null;
                                if ((lastLocation != null) && (!lastLocation.isEmpty())) {
                                    pieces = lastLocation.split("/");
                                }
                                httpget.addHeader("X-Correlation-Id", "https://export.enphaseenergy.com/index.php"); // Silly Security measure used by enphase. May change
                                response.close();
                                response = null;
                                httpget.setURI(new URI("https://enlighten.enphaseenergy.com/systems/" + _systemId + "/inverter_status.json"));
                                response = httpclient.execute(httpget);

                                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                                    rd = new BufferedReader(
                                            new InputStreamReader(response.getEntity().getContent()));
                                    result = new StringBuffer();
                                    line = "";
                                    while ((line = rd.readLine()) != null) {
                                        result.append(line);
                                    }
                                    h = response.getAllHeaders();
                                    JSONObject invs = new JSONObject(result.toString());
                                    JSONArray invsArr = invs.names();
                                    ArrayList<String> reInverters = new ArrayList<>();
                                    for (int i = 0; i < invsArr.length(); i++) {
                                        reInverters.add(invsArr.getString(i));
                                    }

                                    // Now ,there is a list of inverters
                                    // We have reference information about the inverters
                                    // Now get some data
                                    // Data to retrieve :
                                    // AC output Power
                                    // AC output voltage (calculated)
                                    // AC output current
                                    // DC input power (calculated)
                                    // DC input voltage
                                    // DC input current 
                                    // The data would be gathered day by day
                                    // So first we must get the first day to extract
                                    // Start from the last date to today
                                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                                    String[] inverters = reInverters.toArray(new String[0]);
                                    String stat = URLEncoder.encode("POWR,DCV,DCA,ACV,ACHZ,TMPI", "UTF-8");
                                    builder = RequestBuilder.get();

                                    Calendar now = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                                    long stopPoint = 0;
                                    long startDK = 0;
                                    // Start getting data                                     
                                    SimpleDateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                    for (String invert : inverters) {
                                        long startDate = format.parse(latestTimestamp).getTime();

                                        // For each inverter highlighted, retrieve the required data
                                        // Shift the required date 1 day backward from today if its today
                                        Calendar target = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                                        long today = target.getTimeInMillis();

                                        // Give 10 minutes away
                                        today = today - (10 * 60 * 1000);

                                        // Don't get more than 5 days worth of data
                                        stopPoint = today;
                                        //stopPoint = startDate + (86400000 * 5);
                                        if (stopPoint > today) {
                                            stopPoint = today;
                                        }
                                        format = new SimpleDateFormat("yyyy-MM-dd");
                                        startDK = startDate;
                                        int dbCount = 0;

                                        {
                                            col = _db.getCollection(GetCollectionName("inverter_data"));
                                            DBCollection col0 = _db.getCollection(GetCollectionName("inverter_daily_data"));
                                            DBCollection col1 = _db.getCollection(GetCollectionName("system_daily_data"));
                                            DBCollection col2 = _db.getCollection(GetCollectionName("system_data"));
                                            DBCollection col3 = _db.getCollection(GetCollectionName("irradiance_data"));

                                            BulkWriteOperation mongoBuilder = col.initializeOrderedBulkOperation();
                                            BulkWriteOperation mongoBuilder0 = col0.initializeOrderedBulkOperation();
                                            BulkWriteOperation mongoBuilder1 = col1.initializeOrderedBulkOperation();
                                            BulkWriteOperation mongoBuilder2 = col2.initializeOrderedBulkOperation();
                                            BulkWriteOperation mongoBuilder3 = col3.initializeOrderedBulkOperation();

                                            while ((startDate <= stopPoint) && _running) {
                                                now.setTimeInMillis(startDate);
                                                String start = format.format(now.getTime());
                                                now.setTimeInMillis(startDate + 86400000);
                                                String stop = format.format(now.getTime());
                                                now.setTimeInMillis(startDate);

                                                String loc = "https://enlighten.enphaseenergy.com/systems/" + _systemId + "/inverters/" + invert + "/time_series_x?&stat=" + stat + "&date=" + start;
                                                httpget.setURI(new URI(loc));
                                                response = httpclient.execute(httpget);
                                                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                                                    rd = new BufferedReader(
                                                            new InputStreamReader(response.getEntity().getContent()));
                                                    result = new StringBuffer();
                                                    line = "";
                                                    while ((line = rd.readLine()) != null) {
                                                        result.append(line);
                                                    }
                                                    response.close();
                                                    response = null;

                                                    JSONObject obj = new JSONObject(result.toString());
                                                    if (!obj.has("POWR")) {
                                                        startDate = startDate + 86400000;
                                                        continue;
                                                    }

                                                    JSONArray powr = obj.getJSONArray("POWR");
                                                    JSONArray acv = obj.getJSONArray("ACV");
                                                    JSONArray dcv = obj.getJSONArray("DCV");
                                                    JSONArray dca = obj.getJSONArray("DCA");
                                                    JSONArray achz = obj.getJSONArray("ACHZ");
                                                    JSONArray tmpi = obj.getJSONArray("TMPI");
                                                    int len = powr.length();
                                                    if (acv.length() < len) {
                                                        len = acv.length();
                                                    }
                                                    if (dca.length() < len) {
                                                        len = dca.length();
                                                    }
                                                    if (dcv.length() < len) {
                                                        len = dcv.length();
                                                    }
                                                    if (achz.length() < len) {
                                                        len = achz.length();
                                                    }
                                                    if (tmpi.length() < len) {
                                                        len = tmpi.length();
                                                    }

                                                    for (int i = 0; i < len; i++) {
                                                        JSONArray _powr = powr.getJSONArray(i);
                                                        JSONArray _acv = acv.getJSONArray(i);
                                                        JSONArray _dcv = dcv.getJSONArray(i);
                                                        JSONArray _dca = dca.getJSONArray(i);
                                                        JSONArray _achz = achz.getJSONArray(i);
                                                        JSONArray _tmpi = tmpi.getJSONArray(i);

                                                        Double acpower = (_powr.isNull(1)) ? null : _powr.getDouble(1);
                                                        Double acvoltage = (_acv.isNull(1)) ? null : _acv.getDouble(1);
                                                        Double accurrent = ((acpower == null) || (acvoltage == null)) ? null : ((acpower) / (acvoltage));
                                                        Double dcvoltage = (_dcv.isNull(1)) ? null : _dcv.getDouble(1);
                                                        Double dccurrent = (_dca.isNull(1)) ? null : _dca.getDouble(1);
                                                        Double dcpower = ((dcvoltage == null) || (dccurrent == null)) ? null : ((dcvoltage) * (dccurrent));
                                                        Double frequency = (_achz.isNull(1)) ? null : _achz.getDouble(1);
                                                        Double temperature = (_tmpi.isNull(1)) ? null : _tmpi.getDouble(1);

                                                        now.setTimeInMillis(_powr.getLong(0) * 1000);

                                                        dbCount += DataInput(col, col0, col1, col2, col3, mongoBuilder, mongoBuilder0,
                                                                mongoBuilder1, mongoBuilder2, mongoBuilder3, now, invert, acpower, dcpower,
                                                                acvoltage, dcvoltage, accurrent, dccurrent, frequency, temperature, null, null,5);

                                                        if (dbCount >= 20) {
                                                            mongoBuilder.execute();
                                                            mongoBuilder0.execute();
                                                            mongoBuilder1.execute();
                                                            mongoBuilder2.execute();
                                                            mongoBuilder3.execute();
                                                            dbCount = 0;
                                                            mongoBuilder = col.initializeOrderedBulkOperation();
                                                            mongoBuilder0 = col0.initializeOrderedBulkOperation();
                                                            mongoBuilder1 = col1.initializeOrderedBulkOperation();
                                                            mongoBuilder2 = col2.initializeOrderedBulkOperation();
                                                            mongoBuilder3 = col3.initializeOrderedBulkOperation();
                                                        }

                                                    }
                                                }
                                                startDate = startDate + 86400000;
                                            }
                                            if (dbCount > 0) {
                                                mongoBuilder.execute();
                                                mongoBuilder0.execute();
                                                mongoBuilder1.execute();
                                                mongoBuilder2.execute();
                                                mongoBuilder3.execute();
                                                dbCount = 0;
                                            }
                                        }
                                    }
                                    // Inverter data has been retrieved,
                                    // Next we need alerts
                                    // We would be using the API key
                                    // Get System Events
                                    if (_apiKey != null) {
                                        String loc = "https://api.enphaseenergy.com/api/systems/" + _systemId + "/alerts?level=low&key=" + _apiKey;
                                        httpget.setURI(new URI(loc));
                                        response = httpclient.execute(httpget);
                                        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                                            rd = new BufferedReader(
                                                    new InputStreamReader(response.getEntity().getContent()));
                                            result = new StringBuffer();
                                            line = "";
                                            while ((line = rd.readLine()) != null) {
                                                result.append(line);
                                            }
                                            response.close();
                                            response = null;

                                            JSONObject contents = new JSONObject(result.toString());
                                            JSONArray alerts = contents.getJSONArray("alerts");
                                            {
                                                int dbCount = 0;
                                                col = _db.getCollection(GetCollectionName("system_events"));
                                                BulkWriteOperation wrt = col.initializeOrderedBulkOperation();

                                                for (int i = 0; i < alerts.length(); i++) {
                                                    long t = alerts.getJSONObject(i).getInt("alert_start");
                                                    t = t * 1000;
                                                    if (t > stopPoint) {
                                                        break;
                                                    }
                                                    if (t > startDK) {
                                                        now.setTimeInMillis(t);
                                                        String time = outFormat.format(now.getTime());
                                                        String deviceCode = null;
                                                        String deviceDesc = null;
                                                        String errorCode = alerts.getJSONObject(i).getString("level");
                                                        String errorDesc = alerts.getJSONObject(i).getString("alert_name");

                                                        /// Database action here
                                                        DBObject testItem = new BasicDBObject("farm_id", _farmId).append("system_id", _systemId)
                                                                .append("timestamp", time);
                                                        boolean found = false;
                                                        try (DBCursor cur = col.find(testItem)) {
                                                            if (cur.hasNext()) {
                                                                found = true;
                                                            }
                                                            cur.close();
                                                        }
                                                        if (!found) {
                                                            DBObject item = new BasicDBObject("farm_id", _farmId)
                                                                    .append("system_id", _systemId)
                                                                    .append("system_name", _systemName)
                                                                    .append("timestamp", time)
                                                                    .append("device_code", deviceCode)
                                                                    .append("device_description", deviceDesc)
                                                                    .append("error_code", errorCode)
                                                                    .append("error_description", errorDesc)
                                                                    .append("duration", null)
                                                                    .append("checked", false);
                                                            wrt.insert(item);
                                                            dbCount++;
                                                        }

                                                        if (dbCount >= 20) {
                                                            wrt.execute();
                                                            wrt = col.initializeOrderedBulkOperation();
                                                            dbCount = 0;
                                                        }
                                                    }
                                                }
                                                if (dbCount > 0) {
                                                    wrt.execute();
                                                }

                                            }
                                        }
                                    }

                                }

                                /* DEBUG */
                                //System.out.println(result);
                            }
                        }
                    } catch (IOException | URISyntaxException ex) {
                        Logger.getLogger(EnphaseModelSite.class.getName()).log(Level.SEVERE, null, ex);
                    } finally {
                        try {
                            if (response != null) {
                                response.close();
                            }
                            httpclient.close();
                        } catch (IOException ex1) {
                            Logger.getLogger(EnphaseModelSite.class.getName()).log(Level.SEVERE, null, ex1);
                        }
                    }
                } catch (ParseException ex) {
                    Logger.getLogger(EnphaseModelSite.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        } finally {
            Stop();
        }
    }

}
