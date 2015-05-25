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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
 * @version 1.0 2015-05-24 Class to model acquisition from Fronius Sites
 */
public class FroniusModelSite extends ModelSite {

    public FroniusModelSite(String tablePrefix, String systemId, String systemName, String username, String password, int farmId, DB db) {
        super(tablePrefix, systemId, systemName, username, password, farmId, db);
    }

    @Override
    public void run() {
        try {
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
                    // Don't do anything if the time in the database is not a day away from now
                    Calendar ca = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                    Calendar ca0 = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                    SimpleDateFormat formati = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    ca0.setTime(formati.parse(latestTimestamp));
                    if ((ca.getTimeInMillis() - ca0.getTimeInMillis()) < (24 * 60 * 60 * 1000)) {
                        return;
                    }

                    // Connect to the web server - Always redirect
                    CloseableHttpClient httpclient = HttpClientBuilder.create().setRedirectStrategy(new LaxRedirectStrategy()).build();

                    // Set headers
                    CloseableHttpResponse response = null;
                    HttpGet httpget = new HttpGet("https://www.solarweb.com/");
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
                                    if ("username".equals(element_name.toLowerCase())) {
                                        post_data.put(element_name, _username);
                                    } else if ("password".equals(element_name.toLowerCase())) {
                                        post_data.put(element_name, _password);
                                    } else {
                                        post_data.put(element_name, element.attr("value"));
                                    }
                                }
                            }
                            // Build the query
                            RequestBuilder builder = RequestBuilder.post().setUri(new URI("https://www.solarweb.com/Account/LogOn"));
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

                            // Response must be good;
                            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                                // Verify login was successfull
                                boolean success = false;
                                String[] pieces = null;
                                String[] rpieces = null;
                                if ((lastLocation != null) && (!lastLocation.isEmpty())) {
                                    pieces = lastLocation.split("/");
                                }
                                if (pieces != null) {
                                    ArrayList<String> tempPiece = new ArrayList<>();
                                    for (String piece : pieces) {
                                        if (!piece.isEmpty()) {
                                            tempPiece.add(piece.trim());
                                        }
                                    }
                                    rpieces = tempPiece.toArray(new String[tempPiece.size()]);
                                }
                                if ((pieces != null) && (rpieces != null) && (pieces.length > 0) && (rpieces[rpieces.length - 1] == null ? _systemId == null : rpieces[rpieces.length - 1].equals(_systemId)) && !_systemId.isEmpty()) {
                                    // For the login to be successful, the above condition must be met
                                    success = true;
                                }

                                rd = new BufferedReader(
                                        new InputStreamReader(response.getEntity().getContent()));
                                result = new StringBuffer();
                                line = "";
                                while ((line = rd.readLine()) != null) {
                                    result.append(line);
                                }
                                doc = Jsoup.parse(result.toString());
                                response.close();
                                response = null;
                                String htmlText = doc.html();
                                String installDate = "";
                                Matcher matcher = Pattern.compile("pvSystemImageDate\">(\\s?\\d+.\\d+.\\d+)</").matcher(htmlText);
                                while (matcher.find()) {
                                    installDate = matcher.group(1);
                                }

                                // We are on the home page
                                // Lets navigate to the chart page
                                builder = RequestBuilder.get().setUri(new URI("https://www.solarweb.com/NewCharts/Chart/" + _systemId + "/"));
                                HttpUriRequest browser = builder.build();
                                response = httpclient.execute(browser);
                                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                                    // Navigation done
                                    rd = new BufferedReader(
                                            new InputStreamReader(response.getEntity().getContent()));
                                    result = new StringBuffer();
                                    line = "";
                                    while ((line = rd.readLine()) != null) {
                                        result.append(line);
                                    }
                                    doc = Jsoup.parse(result.toString());
                                    response.close();
                                    response = null;
                                    // Get some web data
                                    htmlText = doc.html();
                                    String groupId = "";
                                    matcher = Pattern.compile("groupId\\s?=\\s?\"(.+?)\";").matcher(htmlText);
                                    while (matcher.find()) {
                                        groupId = matcher.group(1);
                                    }

                                    if ((!groupId.isEmpty()) && (!_systemId.isEmpty())) {
                                        SimpleDateFormat format = new SimpleDateFormat("yyyy/M/6");
                                        Calendar date = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                                        String loc = "https://www.solarweb.com/NewCharts/GetChartData/" + _systemId + "/" + groupId + "/Day/" + format.format(date.getTime()) + "?isStacked=true&channel=Unknown&isAutoscale=false";
                                        builder = RequestBuilder.get().setUri(new URI(loc));
                                        browser = builder.build();
                                        response = httpclient.execute(browser);
                                        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                                            // List of inverters
                                            ArrayList<Map<String, String>> reInverters = new ArrayList<>();

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
                                            JSONArray arr = obj.getJSONObject("chart").getJSONArray("series");
                                            for (int i = 0; i < arr.length(); i++) {
                                                JSONObject value = arr.getJSONObject(i);
                                                if (value.getString("stack").equals("data")) {
                                                    Map<String, String> inv = new HashMap<>();
                                                    inv.put("id", value.getString("deviceId"));
                                                    inv.put("name", value.getString("deviceName"));
                                                    reInverters.add(inv);
                                                }
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
                                            format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                            long startDate = format.parse(latestTimestamp).getTime();

                                            // For each inverter highlighted, retrieve the required data
                                            // Shift the required date 1 day backward from today if its today
                                            Calendar target = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                                            if (target.get(Calendar.HOUR_OF_DAY) > 12) {
                                                target.add(Calendar.DAY_OF_YEAR, -1);
                                            } else {
                                                target.add(Calendar.DAY_OF_YEAR, -2);
                                            }
                                            target.set(Calendar.HOUR_OF_DAY, 23);
                                            target.set(Calendar.MINUTE, 59);
                                            long today = target.getTimeInMillis();

                                            // Don't get more than 5 days worth of data
                                            long stopPoint = today;
                                            //stopPoint = startDate + (86400000 * 5);
                                            if (stopPoint > today) {
                                                stopPoint = today;
                                            }
                                            long latestT = startDate;
                                            Calendar now = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                                            format = new SimpleDateFormat("yyyy/M/d");

                                            // Start getting data 
                                            SimpleDateFormat dformat = new SimpleDateFormat("D");
                                            SimpleDateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                            int dtr = target.get(Calendar.ZONE_OFFSET);
                                            boolean valueadded = false;

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
                                                    now.setTimeInMillis(startDate - dtr);
                                                    String start = format.format(now.getTime());
                                                    now.setTimeInMillis(startDate + 86400000 - dtr);
                                                    String stop = format.format(now.getTime());
                                                    now.setTimeInMillis(startDate - dtr);

                                                    // First up is the AC output power
                                                    loc = "https://www.solarweb.com/NewCharts/GetChartData/";
                                                    loc = loc + _systemId + "/" + groupId + "/Day/" + start + "?isStacked=false&channel=Unknown&isAutoscale=true";
                                                    builder = RequestBuilder.get().setUri(new URI(loc));
                                                    browser = builder.build();
                                                    response = httpclient.execute(browser);
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
                                                        obj = new JSONObject(result.toString());
                                                        arr = obj.getJSONObject("chart").getJSONArray("series");
                                                        JSONArray dayData = new JSONArray();
                                                        for (int i = 0; i < arr.length(); i++) {
                                                            JSONObject value = arr.getJSONObject(i);
                                                            if (value.getString("stack").equals("data")) {
                                                                dayData.put(value);
                                                            }
                                                        }
                                                        for (int i = 0; i < reInverters.size(); i++) {
                                                            Map<String, String> inv = reInverters.get(i);
                                                            JSONArray power_data = new JSONArray();
                                                            JSONArray acv_data = new JSONArray();
                                                            JSONArray dcc_data = new JSONArray();
                                                            JSONArray dcv_data = new JSONArray();
                                                            for (int j = 0; j < dayData.length(); j++) {
                                                                JSONObject val = dayData.getJSONObject(j);
                                                                if (val.getString("id").equals(inv.get("id") + "-Power")) {
                                                                    power_data.put(val.getJSONArray("data"));
                                                                } else if (val.getString("id").equals(inv.get("id") + "-UACMeanL1")) {
                                                                    acv_data.put(val.getJSONArray("data"));
                                                                } else if (val.getString("id").equals(inv.get("id") + "-UDCMean")) {
                                                                    dcv_data.put(val.getJSONArray("data"));
                                                                } else if (val.getString("id").equals(inv.get("id") + "-IDCMean")) {
                                                                    dcc_data.put(val.getJSONArray("data"));
                                                                }
                                                            }

                                                            // Retrieved the 
                                                            for (int b = 0; b < power_data.length(); b++) {
                                                                for (int j = 0; j < power_data.getJSONArray(b).length(); j++) {
                                                                    // Put data in..
                                                                    now.setTimeInMillis(power_data.getJSONArray(b).getJSONArray(j).getLong(0) - dtr);
                                                                    Object acpower = (power_data.getJSONArray(b).getJSONArray(j).isNull(1)) ? null : power_data.getJSONArray(b).getJSONArray(j).getDouble(1) * 1000.0;
                                                                    Object acvoltage = (acv_data.getJSONArray(b).getJSONArray(j).isNull(1)) ? null : acv_data.getJSONArray(b).getJSONArray(j).getDouble(1);
                                                                    Object accurrent = ((acpower == null) || (acvoltage == null)) ? null : (((Double) acpower) / ((Double) acvoltage));
                                                                    Object dcvoltage = (dcv_data.getJSONArray(b).getJSONArray(j).isNull(1)) ? null : dcv_data.getJSONArray(b).getJSONArray(j).getDouble(1);
                                                                    Object dccurrent = (dcc_data.getJSONArray(b).getJSONArray(j).isNull(1)) ? null : dcc_data.getJSONArray(b).getJSONArray(j).getDouble(1);
                                                                    Object dcpower = ((dcvoltage == null) || (dccurrent == null)) ? null : (((Double) dcvoltage) * ((Double) dccurrent));

                                                                    dbCount += DataInput(col, col0, col1, col2, col3, mongoBuilder, mongoBuilder0,
                                                                            mongoBuilder1, mongoBuilder2, mongoBuilder3, now, inv.get("id"),
                                                                            Double.valueOf(acpower.toString()), Double.valueOf(dcpower.toString()),
                                                                            Double.valueOf(acvoltage.toString()), Double.valueOf(dcvoltage.toString()),
                                                                            Double.valueOf(accurrent.toString()), Double.valueOf(dccurrent.toString()),
                                                                            null, 0.0, null, null, 10);

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

                                                                    //System.out.println("time:" + outFormat.format(now.getTime()));
                                                                    //System.out.println("acpower:" + ((acpower == null) ? "NULL" : acpower));
                                                                    //System.out.println("acvoltage: " + ((acvoltage == null) ? "NULL" : acvoltage));
                                                                    //System.out.println("accurrent: " + ((accurrent == null) ? "NULL" : accurrent));
                                                                    //System.out.println("dcvoltage: " + ((dcvoltage == null) ? "NULL" : dcvoltage));
                                                                    //System.out.println("dccurrent: " + ((dccurrent == null) ? "NULL" : dccurrent));
                                                                    //System.out.println("dcpower: " + ((dcpower == null) ? "NULL" : dcpower));                                                            
                                                                }
                                                                valueadded = true;
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
                                            // Retrieved inverter data
                                            // Now we retrieve the events
                                            loc = "https://www.solarweb.com/Home/MenuServiceMessages/" + _systemId;
                                            builder = RequestBuilder.get().setUri(new URI(loc));
                                            browser = builder.build();
                                            response = httpclient.execute(browser);
                                            response.close();
                                            response = null;

                                            int echo = 1;
                                            int iSt = 0;
                                            int iLen = 100;

                                            loc = "https://www.solarweb.com/Home/GetServiceMessages/" + _systemId;
                                            loc += "?sEcho=" + echo;
                                            loc += "&iColumns=5&sColumns=";
                                            loc += "&iDisplayStart=" + iSt;
                                            loc += "&iDisplayLength=" + iLen;
                                            loc += "&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&mDataProp_3=3&mDataProp_4=4&sSearch=&bRegex=false&sSearch_0=&bRegex_0=false&bSearchable_0=true&sSearch_1=&bRegex_1=false&bSearchable_1=true&sSearch_2=&bRegex_2=false&bSearchable_2=true&sSearch_3=&bRegex_3=false&bSearchable_3=true&sSearch_4=&bRegex_4=false&bSearchable_4=true&iSortCol_0=0&sSortDir_0=desc&iSortingCols=1&bSortable_0=true&bSortable_1=false&bSortable_2=false&bSortable_3=false&bSortable_4=false";
                                            loc += "&_=" + Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis();
                                            builder = RequestBuilder.get().setUri(new URI(loc));
                                            browser = builder.build();
                                            response = httpclient.execute(browser);
                                            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                                                rd = new BufferedReader(
                                                        new InputStreamReader(response.getEntity().getContent()));
                                                result = new StringBuffer();
                                                line = "";
                                                while ((line = rd.readLine()) != null) {
                                                    result.append(line);
                                                }
                                                obj = new JSONObject(result.toString());
                                                int total = obj.getInt("iTotalRecords");
                                                response.close();
                                                response = null;
                                                format = new SimpleDateFormat("M/d/yyyy H:mm a");
                                                while ((iSt + iLen) < total) {
                                                    echo += 1;
                                                    iSt += iLen;
                                                }

                                                {
                                                    dbCount = 0;

                                                    col = _db.getCollection(GetCollectionName("system_events"));
                                                    BulkWriteOperation wrt = col.initializeOrderedBulkOperation();

                                                    while ((valueadded && (total > 0)) && _running) {
                                                        loc = "https://www.solarweb.com/Home/GetServiceMessages/" + _systemId;
                                                        loc += "?sEcho=" + echo;
                                                        loc += "&iColumns=5&sColumns=";
                                                        loc += "&iDisplayStart=" + iSt;
                                                        loc += "&iDisplayLength=" + iLen;
                                                        loc += "&mDataProp_0=0&mDataProp_1=1&mDataProp_2=2&mDataProp_3=3&mDataProp_4=4&sSearch=&bRegex=false&sSearch_0=&bRegex_0=false&bSearchable_0=true&sSearch_1=&bRegex_1=false&bSearchable_1=true&sSearch_2=&bRegex_2=false&bSearchable_2=true&sSearch_3=&bRegex_3=false&bSearchable_3=true&sSearch_4=&bRegex_4=false&bSearchable_4=true&iSortCol_0=0&sSortDir_0=desc&iSortingCols=1&bSortable_0=true&bSortable_1=false&bSortable_2=false&bSortable_3=false&bSortable_4=false";
                                                        loc += "&_=" + Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis();
                                                        builder = RequestBuilder.get().setUri(new URI(loc));
                                                        browser = builder.build();
                                                        response = httpclient.execute(browser);
                                                        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                                                            rd = new BufferedReader(
                                                                    new InputStreamReader(response.getEntity().getContent()));
                                                            result = new StringBuffer();
                                                            line = "";
                                                            while ((line = rd.readLine()) != null) {
                                                                result.append(line);
                                                            }
                                                            obj = new JSONObject(result.toString());

                                                            arr = obj.getJSONArray("aaData");
                                                            for (int i = 0; i < arr.length(); i++) {
                                                                JSONArray value = arr.getJSONArray(i);
                                                                Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                                                                cal.setTimeInMillis(today - dtr);
                                                                cal.setTime(format.parse(value.getString(0)));
                                                                if (cal.getTimeInMillis() >= ((latestT - dtr) + (86400000 * 5))) {
                                                                    break;
                                                                }
                                                                if (cal.getTimeInMillis() > (latestT - dtr)) {
                                                                    now.setTimeInMillis(cal.getTimeInMillis());
                                                                    String time = outFormat.format(now.getTime());
                                                                    //System.out.println(time);
                                                                    String deviceCode = (value.getString(1) == null) ? null : value.getString(1);
                                                                    String deviceDesc = (value.getString(2) == null) ? null : value.getString(2);
                                                                    String errorCode = (value.getString(3) == null) ? null : value.getString(3);
                                                                    String errorDesc = (value.getString(4) == null) ? null : value.getString(4);

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

                                                                    //System.out.println("time: " + (String) time);
                                                                    //System.out.println("device_code: " + (String) deviceCode);
                                                                    //System.out.println("device_description: " + (String) deviceDesc);
                                                                    //System.out.println("error_code: " + (String) errorCode);
                                                                    //System.out.println("error_description: " + (String) errorDesc);
                                                                }
                                                            }
                                                            echo += 1;
                                                            iSt += iLen;
                                                            if (iSt > total) {
                                                                break;
                                                            }
                                                        } else {
                                                            break;
                                                        }
                                                    }
                                                    if (dbCount > 0) {
                                                        wrt.execute();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                /* DEBUG */
                                //System.out.println(result);
                            }
                        }

                    } catch (IOException | URISyntaxException | ParseException ex) {
                        Logger.getLogger(FroniusModelSite.class.getName()).log(Level.SEVERE, null, ex);
                    } finally {
                        try {
                            if (response != null) {
                                response.close();
                            }
                            httpclient.close();
                        } catch (IOException ex1) {
                            Logger.getLogger(FroniusModelSite.class.getName()).log(Level.SEVERE, null, ex1);
                        }
                    }
                } catch (ParseException ex) {
                    Logger.getLogger(FroniusModelSite.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        } finally {
            Stop();
        }
    }

}
