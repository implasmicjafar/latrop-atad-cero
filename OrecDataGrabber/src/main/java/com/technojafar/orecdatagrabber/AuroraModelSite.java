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
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
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
 * @version 1.0 2015-05-24 Class to model acquisition from Aurora Sites
 */
public class AuroraModelSite extends ModelSite {

    private int evstart = 0;

    public AuroraModelSite(String tablePrefix, String systemId, String systemName, String username, String password, int farmId, DB db) {
        super(tablePrefix, systemId, systemName, username, password, farmId, db);
    }

    @Override
    public void run() {
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

                ArrayList<SystemLogger> loggers = new ArrayList<>();
                ArrayList<Inverter> inverters = new ArrayList<>();

                // Connect to the web server - Always redirect
                CloseableHttpClient httpclient = HttpClientBuilder.create()
                        .setDefaultCookieStore(new BasicCookieStore())
                        .setRedirectStrategy(new LaxRedirectStrategy())
                        .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.BROWSER_COMPATIBILITY).build())
                        .build();

                // Set headers
                CloseableHttpResponse response = null;
                HttpClientContext contexti = HttpClientContext.create();
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
                    response = httpclient.execute(httpget, contexti);
                    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                        BufferedReader rd = new BufferedReader(
                                new InputStreamReader(response.getEntity().getContent()));
                        StringBuffer result = new StringBuffer();
                        String line = "";
                        while ((line = rd.readLine()) != null) {
                            result.append(line);
                        }
                        Header[] h = response.getAllHeaders();
                        // Close the response
                        response.close();
                        response = null;
                        Document doc = Jsoup.parse(result.toString());
                        // Get input fields
                        Elements elements = doc.getElementsByTag("input");
                        Element form = doc.getElementById("frmLogin");
                        String form_action = "https://accounts.auroravision.net" + form.attr("action");
                        String[] p = form_action.split("\\?");

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
                        String[] params = p[1].split("&");
                        for (String param : params) {
                            String name = param.split("=")[0];
                            String value = param.split("=")[1];
                            post_data.put(name, value);
                        }

                        RequestBuilder builder = RequestBuilder.post().setUri(new URI(form_action));
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
                        h = response.getAllHeaders();
                        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                            if (lastLocation.toLowerCase().endsWith("jsf")) {
                                response.close();
                                response = null;
                                String loc = "https://www.auroravision.net/dash/services/gai/plants.json?entityId=" + _systemId;
                                httpget.setURI(new URI(loc));
                                for (Header hea : h) {
                                    httpget.addHeader(hea);
                                }

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
                                    JSONObject info = new JSONObject(result.toString());
                                    long site_id = info.getJSONArray("result").getJSONObject(0).getLong("entityId");
                                    loc = "https://www.auroravision.net/dash/V2/assets/plantSummary.jsf#" + site_id;
                                    httpget.setURI(new URI(loc));
                                    response = httpclient.execute(httpget);

                                    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                                        // Next, get a list of inverters and loggers. Its important
                                        response.close();
                                        response = null;
                                        loc = "https://www.auroravision.net/dash/services/gai/plant/hierarchy.json?entityId=" + site_id + "&v=2.70.1";
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
                                            JSONObject plant_info = new JSONObject(result.toString());
                                            JSONArray arr0 = plant_info.getJSONObject("result").getJSONArray("descendants");
                                            for (int i = 0; i < arr0.length(); i++) {
                                                String tmp = arr0.getJSONObject(i).getJSONArray("categories").getString(0);
                                                if (tmp.equals("Logger")) {
                                                    SystemLogger aL = new SystemLogger();
                                                    aL.entity_id = arr0.getJSONObject(i).getLong("entityId");
                                                    aL.state = arr0.getJSONObject(i).getString("state");
                                                    aL.manufacturer = arr0.getJSONObject(i).getString("manufacturer");
                                                    aL.model = arr0.getJSONObject(i).getString("model");
                                                    aL.loggerid = arr0.getJSONObject(i).getString("loggerId");
                                                    loggers.add(aL);
                                                    JSONArray arr1 = arr0.getJSONObject(i).getJSONArray("descendants");
                                                    for (int j = 0; j < arr1.length(); j++) {
                                                        tmp = arr1.getJSONObject(j).getJSONArray("categories").getString(0);
                                                        if (tmp.equals("Inverter")) {
                                                            Inverter aI = new Inverter();
                                                            aI.display_name = arr1.getJSONObject(j).getJSONObject("meta").getString("displayName");
                                                            aI.entity_id = arr1.getJSONObject(j).getLong("entityId");
                                                            aI.setFields(arr1.getJSONObject(j).getJSONArray("fields"));
                                                            aI.state = arr1.getJSONObject(j).getString("state");
                                                            aI.active = arr1.getJSONObject(j).getBoolean("active");
                                                            aI.manufacturer = arr1.getJSONObject(j).getString("manufacturer");
                                                            aI.model = arr1.getJSONObject(j).getString("model");
                                                            aI.serial_number = arr1.getJSONObject(j).getString("serialNumber");
                                                            aI.device_id = arr1.getJSONObject(j).getString("deviceId");
                                                            inverters.add(aI);
                                                        }
                                                    }
                                                } else if (tmp.equals("Inverter")) {
                                                    Inverter aI = new Inverter();
                                                    aI.display_name = arr0.getJSONObject(i).getJSONObject("meta").getString("displayName");
                                                    aI.entity_id = arr0.getJSONObject(i).getLong("entityId");
                                                    aI.setFields(arr0.getJSONObject(i).getJSONArray("fields"));
                                                    aI.state = arr0.getJSONObject(i).getString("state");
                                                    aI.active = arr0.getJSONObject(i).getBoolean("active");
                                                    aI.manufacturer = arr0.getJSONObject(i).getString("manufacturer");
                                                    aI.model = arr0.getJSONObject(i).getString("model");
                                                    aI.serial_number = arr0.getJSONObject(i).getString("serialNumber");
                                                    aI.device_id = arr0.getJSONObject(i).getString("deviceId");
                                                    inverters.add(aI);
                                                }
                                            }
                                            // We have reference information about the inverters and loggers
                                            // Now get some data
                                            // Data to retrieve :
                                            // Irradiance data
                                            // Temperature data
                                            // AC output Power
                                            // AC output voltage
                                            // AC output current
                                            // DC input power
                                            // DC input voltage
                                            // DC input current (resolved from the previous two)
                                            // The data would be gathered day by day
                                            // So first we must get the first day to extract
                                            // Start from the last date to today
                                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                            Calendar target = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                                            target.set(Calendar.HOUR_OF_DAY, 23);
                                            Calendar cal1 = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                                            cal1.setTime(formati.parse(info.getJSONArray("result").getJSONObject(0).getString("firstReportedDate")));
                                            if (ca0.before(cal1)) {
                                                ca0.setTime(cal1.getTime());
                                            }
                                            long startDate = ca0.getTimeInMillis();
                                            long today = target.getTimeInMillis();
                                            latestTimestamp = format.format(ca0.getTime());
                                            // Don't get more than 5 days worth of data
                                            long stopPoint = today;
                                            //stopPoint = startDate + (86400000 * 5);
                                            if (stopPoint > today) {
                                                stopPoint = today;
                                            }
                                            Calendar now = Calendar.getInstance(TimeZone.getTimeZone("America/Toronto"));
                                            format = new SimpleDateFormat("yyyyMMdd");

                                            // Start getting data                                     
                                            SimpleDateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                            long startDk = startDate;

                                            String inverter_list = "";
                                            long[] inverts = new long[inverters.size()];
                                            for (int i = 0; i < inverters.size(); i++) {
                                                if (i > 0) {
                                                    inverter_list += ",";
                                                }
                                                inverter_list += inverters.get(i).entity_id + "";
                                                inverts[i] = inverters.get(i).entity_id;
                                            }
                                            inverter_list = URLEncoder.encode(inverter_list, "UTF-8");
                                            {
                                                int dbCount = 0;
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
                                                    long lgb = startDate + 86400000;
                                                    if (lgb > today) {
                                                        lgb = today;
                                                    }
                                                    now.setTimeInMillis(startDate + 86400000);
                                                    String stop = format.format(now.getTime());
                                                    now.setTimeInMillis(startDate);

                                                    JSONObject ac_output_power = null;
                                                    JSONObject ac_output_voltage = null;
                                                    JSONObject ac_output_current = null;
                                                    JSONObject dc_input_power = null;
                                                    JSONObject dc_input_voltage = null;
                                                    JSONObject irradiance = null;
                                                    JSONObject ambient = null;
                                                    JSONObject systemTemp = null;
                                                    // First up is AC output power
                                                    loc = "https://www.auroravision.net/dash/services/gmi/summary/GenerationPower.json?summary=N&type=GenerationPower&start=" + start + "&end=" + stop + "&tz=Canada%2FEastern&eids=" + inverter_list + "&v=2.70.1";
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
                                                        ac_output_power = new JSONObject(result.toString());
                                                    } else {
                                                        break;
                                                    }

                                                    // Next AC output voltage
                                                    loc = "https://www.auroravision.net/dash/services/gmi/summary/Voltage.json?summary=N&type=Voltage&start=" + start + "&end=" + stop + "&tz=Canada%2FEastern&eids=" + inverter_list + "&v=2.70.1";
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
                                                        ac_output_voltage = new JSONObject(result.toString());
                                                    } else {
                                                        break;
                                                    }

                                                    // Next AC Output Current
                                                    loc = "https://www.auroravision.net/dash/services/gmi/summary/Current.json?summary=N&type=Current&start=" + start + "&end=" + stop + "&tz=Canada%2FEastern&eids=" + inverter_list + "&v=2.70.1";
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
                                                        ac_output_current = new JSONObject(result.toString());
                                                    } else {
                                                        break;
                                                    }

                                                    // Next DC input power
                                                    loc = "https://www.auroravision.net/dash/services/gmi/summary/DCGenerationPower.json?summary=N&type=DCGenerationPower&start=" + start + "&end=" + stop + "&tz=Canada%2FEastern&eids=" + inverter_list + "&v=2.70.1";
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
                                                        dc_input_power = new JSONObject(result.toString());
                                                    } else {
                                                        break;
                                                    }

                                                    // Next DC input voltage
                                                    loc = "https://www.auroravision.net/dash/services/gmi/summary/DCVoltage.json?summary=N&type=DCVoltage&start=" + start + "&end=" + start + "&tz=Canada%2FEastern&eids=" + inverter_list + "&v=2.70.1";
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
                                                        dc_input_voltage = new JSONObject(result.toString());
                                                    } else {
                                                        break;
                                                    }

                                                    // Next Irradiance
                                                    loc = "https://www.auroravision.net/dash/services/gmi/summary/Output.json?summary=N&type=Output&start=" + start + "&end=" + stop + "&tz=Canada%2FEastern&eids=" + site_id + "&env=Irradiance&envEid=" + site_id + "&v=2.70.1";
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
                                                        irradiance = output_only(new JSONObject(result.toString()), "Irradiance");
                                                    } else {
                                                        break;
                                                    }

                                                    // Next Ambient temperature
                                                    loc = "https://www.auroravision.net/dash/services/gmi/summary/Output.json?summary=N&type=Output&start=" + start + "&end=" + stop + "&tz=Canada%2FEastern&eids=" + site_id + "&env=AmbientTemp&envEid=" + site_id + "&v=2.70.1";
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
                                                        ambient = output_only(new JSONObject(result.toString()), "AmbientTemp");
                                                    } else {
                                                        break;
                                                    }

                                                    // Next Cell temperature
                                                    loc = "https://www.auroravision.net/dash/services/gmi/summary/Output.json?summary=N&type=Output&start=" + start + "&end=" + stop + "&tz=Canada%2FEastern&eids=" + site_id + "&env=CellTemp&envEid=" + site_id + "&v=2.70.1";
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
                                                        systemTemp = output_only(new JSONObject(result.toString()), "CellTemp");
                                                    } else {
                                                        break;
                                                    }
                                                    // That we are here means all the data was successfully loaded
                                                    for (int i = 0; i < inverts.length; i++) {
                                                        int key = -1;
                                                        JSONArray karr0 = ac_output_power.getJSONArray("fields");
                                                        for (int k = 0; k < karr0.length(); k++) {
                                                            long kl = karr0.getJSONObject(k).getLong("entityId");
                                                            if (kl == inverts[i]) {
                                                                key = k;
                                                                break;
                                                            }
                                                        }

                                                        // if the index was found
                                                        if (key != -1) {
                                                            karr0 = ac_output_power.getJSONArray("fields").getJSONObject(key).getJSONArray("values");
                                                            for (int k = 0; k < karr0.length(); k++) {

                                                                JSONObject _powr = new JSONObject();
                                                                JSONObject _acv = new JSONObject();
                                                                JSONObject _aca = new JSONObject();
                                                                JSONObject _dcv = new JSONObject();
                                                                JSONObject _dcp = new JSONObject();
                                                                JSONObject _irr = new JSONObject();
                                                                JSONObject _tmpa = new JSONObject();
                                                                JSONObject _tmpi = new JSONObject();

                                                                try {
                                                                    _powr = ac_output_power.getJSONArray("fields").getJSONObject(key).getJSONArray("values").getJSONObject(k);
                                                                } catch (Exception ej) {
                                                                }
                                                                try {
                                                                    _acv = ac_output_voltage.getJSONArray("fields").getJSONObject(key).getJSONArray("values").getJSONObject(k);
                                                                } catch (Exception ej) {
                                                                }
                                                                try {
                                                                    _aca = ac_output_current.getJSONArray("fields").getJSONObject(key).getJSONArray("values").getJSONObject(k);
                                                                } catch (Exception ej) {
                                                                }
                                                                try {
                                                                    _dcv = dc_input_voltage.getJSONArray("fields").getJSONObject(key).getJSONArray("values").getJSONObject(k);
                                                                } catch (Exception ej) {
                                                                }
                                                                try {
                                                                    _dcp = dc_input_power.getJSONArray("fields").getJSONObject(key).getJSONArray("values").getJSONObject(k);
                                                                } catch (Exception ej) {
                                                                }
                                                                if (key == 0) {
                                                                    try {
                                                                        _irr = irradiance.getJSONArray("fields").getJSONObject(0).getJSONArray("values").getJSONObject(k);
                                                                    } catch (Exception ej) {
                                                                    }
                                                                    try {
                                                                        _tmpa = ambient.getJSONArray("fields").getJSONObject(0).getJSONArray("values").getJSONObject(k);
                                                                    } catch (Exception ej) {
                                                                    }
                                                                    try {
                                                                        _tmpi = systemTemp.getJSONArray("fields").getJSONObject(0).getJSONArray("values").getJSONObject(k);
                                                                    } catch (Exception ej) {
                                                                    }
                                                                }

                                                                String start_Label = rearrange_timestamp(ac_output_power.getJSONArray("fields").getJSONObject(key).getJSONArray("values").getJSONObject(k).getString("startLabel"));

                                                                Double acpower = (_powr.isNull("value")) ? null : _powr.getDouble("value");
                                                                Double acvoltage = (_acv.isNull("value")) ? null : _acv.getDouble("value");
                                                                Double accurrent = (_aca.isNull("value")) ? null : _aca.getDouble("value");
                                                                Double dcvoltage = (_dcv.isNull("value")) ? null : _dcv.getDouble("value");
                                                                Double dcpower = (_dcp.isNull("value")) ? null : _dcp.getDouble("value");
                                                                Double dccurrent = ((dcpower == null) || (dcvoltage == null)) ? null : ((dcpower) / (dcvoltage));
                                                                Double irradiances = (_irr.isNull("value")) ? null : _irr.getDouble("value");
                                                                Double ambientT = (_tmpa.isNull("value")) ? null : _tmpa.getDouble("value");
                                                                Double temperature = (_tmpi.isNull("value")) ? null : _tmpi.getDouble("value");

                                                                Calendar cal = Calendar.getInstance();
                                                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                                                cal.setTime(sdf.parse(start_Label));// all done
                                                                dbCount += DataInput(col, col0, col1, col2, col3, mongoBuilder, mongoBuilder0,
                                                                        mongoBuilder1, mongoBuilder2, mongoBuilder3, cal, (inverts[i] + ""), acpower, dcpower,
                                                                        acvoltage, dcvoltage, accurrent, dccurrent, null, temperature, irradiances, ambientT, 15);

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
                                            // Inverter data has been retrieved,
                                            // Next we need alerts
                                            // Transverse to the error page
                                            response = null;
                                            loc = "http://www.auroravision.net/dash/V2/events/profileEventLog.jsf";
                                            httpget.setURI(new URI(loc));
                                            response = httpclient.execute(httpget);
                                            response.close();
                                            response = null;
                                            long selector = Math.round((today - startDk) / 1000.0);
                                            Integer old = 86400;
                                            if (selector > 0) {
                                                old = 604800;
                                                if (selector > 604800) {
                                                    old = 2592000;
                                                    if (selector > 2592000) {
                                                        old = null;
                                                    }
                                                }
                                            }

                                            int total = -1;
                                            JSONObject payload = new JSONObject();
                                            payload.put("requestType", "EVENTS_RANGE_BY_FILTER");
                                            payload.put("maximumResults", 30);
                                            payload.put("firstEventIndex", evstart);
                                            Map<String, Object> filter = new HashMap<>();
                                            filter.put("configured", "true");
                                            filter.put("ownerId", _systemId);
                                            filter.put("secondsOld", old);
                                            filter.put("siteEIds", new long[]{site_id});
                                            filter.put("sourceEvents", "true");
                                            filter.put("state", null);
                                            payload.put("filter", filter);

                                            loc = "https://www.auroravision.net/dash/services/events/evs/events/list.json";
                                            HttpPost post = new HttpPost(new URI(loc));
                                            for (Header hea : httpget.getAllHeaders()) {
                                                post.addHeader(hea);
                                            }
                                            {
                                                int dbCount = 0;
                                                col = _db.getCollection(GetCollectionName("system_events"));
                                                BulkWriteOperation wrt = col.initializeOrderedBulkOperation();
                                                while (_running) {
                                                    payload.put("firstEventIndex", evstart);
                                                    String strPayload = payload.toString();
                                                    post.setHeader("Content-Type", "application/json");
                                                    post.setHeader("X-Requested-With", "XMLHttpRequest");
                                                    post.setEntity(new StringEntity(strPayload, ContentType.create("application/json")));
                                                    response = httpclient.execute(post);
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
                                                        JSONObject events = new JSONObject(result.toString());
                                                        total = events.getInt("totalNumberOfEvents");
                                                        if (evstart > total) {
                                                            break;
                                                        }
                                                        if (evstart == 0) {
                                                            evstart = 31;
                                                        } else {
                                                            evstart += 30;
                                                        }
                                                        JSONArray eventArr = events.getJSONArray("partialEvents");
                                                        for (int i = 0; i < eventArr.length(); i++) {
                                                            long t = eventArr.getJSONObject(i).getLong("startDate");
                                                            if (t > stopPoint) {
                                                                break;
                                                            } else if (t > startDk) {
                                                                String time = eventArr.getJSONObject(i).getString("formattedStartDate").substring(0, 19);
                                                                String deviceCode = (eventArr.getJSONObject(i).isNull("sourceEventId")) ? null : eventArr.getJSONObject(i).getInt("sourceEventId") + "";
                                                                String deviceDesc = (eventArr.getJSONObject(i).isNull("eventType")) ? null : eventArr.getJSONObject(i).getJSONObject("eventType").getJSONObject("parentEventType").getJSONObject("parentEventType").getString("code");
                                                                String errorCode = (eventArr.getJSONObject(i).isNull("eventType")) ? null : eventArr.getJSONObject(i).getJSONObject("eventType").getJSONObject("parentEventType").getJSONObject("parentEventType").getString("description");
                                                                String errorDesc = (eventArr.getJSONObject(i).isNull("eventType")) ? null : eventArr.getJSONObject(i).getJSONObject("eventType").getJSONObject("parentEventType").getJSONObject("parentEventType").getString("name");
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
                        }
                    }
                } catch (IOException | URISyntaxException | ParseException ex) {
                    Logger.getLogger(AuroraModelSite.class.getName()).log(Level.SEVERE, null, ex);
                } finally {
                    try {
                        if (response != null) {
                            response.close();
                        }
                        httpclient.close();
                    } catch (IOException ex1) {
                        Logger.getLogger(AuroraModelSite.class.getName()).log(Level.SEVERE, null, ex1);
                    }
                }

            } finally {
                Stop();
            }
        }
    }

    /**
     * Function to rearrange the compressed timestamp returned by the json
     * requests into a proper formatted timestamp
     *
     * @param string $timestamp
     * @return string
     */
    private String rearrange_timestamp(String timestamp) {
        String resu = timestamp.substring(0, 4) + "-" + timestamp.substring(4, 6) + "-" + timestamp.substring(6, 8) + " " + timestamp.substring(8, 10) + ":" + timestamp.substring(10, 12) + ":" + timestamp.substring(12, 14);
        return resu;
    }

    class SystemLogger {

        long entity_id = 0;
        String state = "";
        String manufacturer = "";
        String model = "";
        String loggerid = "";
    }

    private JSONObject output_only(JSONObject obj, String atype) {
        JSONArray aArr0 = obj.getJSONArray("fields");
        obj.remove("fields");
        JSONArray aArr1 = new JSONArray();
        for (int i = 0; i < aArr0.length(); i++) {
            String type = aArr0.getJSONObject(i).getString("field");
            if (type.equals(atype)) {
                aArr1.put(aArr0.getJSONObject(i));
            }
        }
        obj.put("fields", aArr1);
        return obj;
    }

    class Inverter {

        String display_name = "";
        long entity_id = 0;
        String[] fields = null;
        String state = "";
        boolean active = false;
        String manufacturer = "";
        String model = "";
        String serial_number = "";
        String device_id = "";

        void setFields(JSONArray arr) {
            fields = new String[arr.length()];
            for (int i = 0; i < arr.length(); i++) {
                fields[i] = arr.getString(i);
            }
        }
    }

}
