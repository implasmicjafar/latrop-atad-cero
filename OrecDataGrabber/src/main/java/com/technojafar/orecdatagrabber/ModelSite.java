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
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 *
 * @author Jafaru Mohammed
 * @version 1.0 2015-05-20 This class is intended to be inherited be other
 * classes that perform specifics as per the site technology. It is a site
 * model.
 *
 */
public abstract class ModelSite implements Runnable {

    private Thread _thread = null;
    protected boolean _running = false;
    protected String _systemId;
    protected String _systemName;
    protected String _username;
    protected String _password;
    protected String _apiKey;
    protected String _tablePrefix;
    protected int _farmId;
    protected DB _db;

    public void setDelay(long _delay) {
        this._delay = _delay;
    }
    protected long _delay = 0;
    protected String _startTime = "";
    protected String _name = "";
    protected long _rundelay = 0;

    /**
     * Constructor for Model Sites without API key.
     *
     * @param tablePrefix
     * @param systemId
     * @param systemName
     * @param username
     * @param password
     * @param farmId
     * @param db
     */
    public ModelSite(String tablePrefix, String systemId, String systemName, String username, String password, int farmId, DB db) {
        _thread = null;
        _running = false;

        _systemId = systemId;
        _systemName = systemName;
        _username = username;
        _password = password;
        _farmId = farmId;
        _db = db;

        _delay = 0;
        _startTime = "";
        _name = "";
        _rundelay = 0;

        _tablePrefix = tablePrefix;
    }

    public void setName(String _name) {
        this._name = _name;
    }

    public void setStartTime(String _startTime) {
        this._startTime = _startTime;
    }

    public void setRundelay(long _rundelay) {
        this._rundelay = _rundelay;
    }

    public long getDelay() {
        return _delay;
    }

    public long getRundelay() {
        return _rundelay;
    }

    /**
     * Constructor for Model site with API key.
     *
     * @param tablePrefix
     * @param systemId
     * @param systemName
     * @param username
     * @param password
     * @param farmId
     * @param apiKey
     * @param db
     */
    public ModelSite(String tablePrefix, String systemId, String systemName, String username, String password, int farmId, String apiKey, DB db) {
        _systemId = systemId;
        _systemName = systemName;
        _username = username;
        _password = password;
        _farmId = farmId;
        _apiKey = apiKey;
        _db = db;

        _thread = null;
        _running = false;

        _delay = 0;
        _startTime = "";
        _name = "";
        _rundelay = 0;

        _tablePrefix = tablePrefix;
    }

    /**
     * Getter for the running state of the thread
     *
     * @return Boolean whether the thread is running
     */
    protected boolean IsRunning() {
        return _running;
    }

    /**
     * Method to start the thread
     */
    protected void Start() {
        if (!_running) {
            _thread = new Thread(this);
            _running = true;
            _delay = 0;
            _thread.start();
        }
    }

    protected String GetCollectionName(String collection) {
        return _tablePrefix + collection;
    }

    /**
     * Method to stop the thread
     */
    protected void Stop() {
        if (_running) {
            _running = false;
            DBCollection coll = _db.getCollection(GetCollectionName("system_data"));
            try (DBCursor cur = coll.find(new BasicDBObject("farm_id", _farmId)).sort(new BasicDBObject("timestamp", 1)).limit(1)) {
                if (cur.hasNext()) {
                    DBCollection coll0 = _db.getCollection(GetCollectionName("system_data_entry_log"));
                    DBObject obj = cur.next();
                    DBObject obj0 = new BasicDBObject("system_name", obj.get("system_name")).append("system_id", obj.get("system_id"));
                    obj0.put("latest_timestamp", obj.get("timestamp"));
                    boolean _op = false;
                    try (DBCursor cur0 = coll0.find(obj0)) {
                        if (cur0.hasNext()) {
                            coll0.update(cur0.next(), obj0);
                        } else {
                            coll0.insert(obj0);
                        }
                        _op = true;
                        cur0.close();
                    }
                    if (!_op) {
                        coll0.insert(obj0);
                    }
                }
                cur.close();
            }
            _running = false;
        }
        _thread = null;
    }

    protected int DataInput(DBCollection col, DBCollection col0, DBCollection col1, DBCollection col2, DBCollection col3,
            BulkWriteOperation mongoBuilder, BulkWriteOperation mongoBuilder0, BulkWriteOperation mongoBuilder1,BulkWriteOperation mongoBuilder2, BulkWriteOperation mongoBuilder3, 
            Calendar now, String invert, Double acpower, Double dcpower, Double acvoltage, Double dcvoltage, Double accurrent, 
            Double dccurrent, Double frequency, Double temperature, Double irradiance, Double amb_temp, int timeint) {
        SimpleDateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        DBObject testItem = new BasicDBObject("farm_id", _farmId)
                .append("system_id", _systemId)
                .append("timestamp", outFormat.format(now.getTime()))
                .append("inverter_id", invert);
        boolean found = false;
        try (DBCursor cur = col.find(testItem)) {
            if (cur.hasNext()) {
                found = true;
            }
            cur.close();
        }
        if (!found) {
            double energ = (acpower == null) ? 0 : (acpower * ((timeint * 1.0) / 60.0));
            {
                DBObject item = new BasicDBObject("farm_id", _farmId)
                        .append("system_name", _systemName)
                        .append("system_id", _systemId)
                        .append("timestamp", outFormat.format(now.getTime()))
                        .append("inverter_id", invert)
                        .append("acpower", acpower)
                        .append("accurrent", acvoltage)
                        .append("dcpower", dcpower)
                        .append("dccurrent", dccurrent)
                        .append("dcvoltage", dcvoltage)
                        .append("linefrequency", frequency)
                        .append("irradiance", irradiance)
                        .append("ambient_temp", amb_temp)
                        .append("system_temp", temperature)
                        .append("acenergy_rec", energ);
                mongoBuilder.insert(item);
            }
            if(irradiance != null){
                testItem = new BasicDBObject("timestamp", outFormat.format(now.getTime()))
                        .append("irradiance_id", _farmId);
                        //.append("irradiance", irradiance);
                DBObject item0 = null;
                try(DBCursor cur0 = col3.find(testItem)){
                    if(cur0.hasNext()){
                        item0 = cur0.next();
                    }
                    cur0.close();
                }
                testItem.put("irradiance", irradiance);
                if(item0 != null) mongoBuilder3.find(item0).update(testItem);
                else mongoBuilder3.insert(testItem);
            }
            {
                testItem = new BasicDBObject("date", format.format(now.getTime()))
                        .append("farm_id", _farmId)
                        .append("system_id", _systemId)
                        .append("inverter_id", invert);
                DBObject item0 = null;
                DBObject item1 = null;
                Double energ0 = 0.0;
                try (DBCursor cur0 = col0.find(testItem)) {
                    if (cur0.hasNext()) {
                        item1 = cur0.next();
                        energ0 = Double.parseDouble((String) item1.get("energy"));
                    }
                    cur0.close();
                }
                energ0 = energ0 + energ;
                item0 = new BasicDBObject("date", format.format(now.getTime()))
                        .append("system_id", _systemId)
                        .append("system_name", _systemName)
                        .append("farm_id", _farmId)
                        .append("inverter_id", invert)
                        .append("energy", energ0);

                if (item1 != null) {
                    mongoBuilder0.find(item1).update(item0);
                } else {
                    mongoBuilder0.insert(item0);
                }
            }
            {
                testItem = new BasicDBObject("date", format.format(now.getTime()))
                        .append("farm_id", _farmId)
                        .append("system_id", _systemId);
                DBObject item0 = null;
                DBObject item1 = null;
                Double energ0 = 0.0;
                try (DBCursor cur0 = col1.find(testItem)) {
                    if (cur0.hasNext()) {
                        item1 = cur0.next();
                        energ0 = Double.parseDouble((String) item1.get("energy"));
                    }
                    cur0.close();
                }
                energ0 = energ0 + energ;
                item0 = new BasicDBObject("date", format.format(now.getTime()))
                        .append("system_id", _systemId)
                        .append("system_name", _systemName)
                        .append("farm_id", _farmId)
                        .append("energy", energ0);

                if (item1 != null) {
                    mongoBuilder1.find(item1).update(item0);
                } else {
                    mongoBuilder1.insert(item0);
                }
            }

            {
                testItem = new BasicDBObject("farm_id", _farmId)
                        .append("system_id", _systemId)
                        .append("timestamp", outFormat.format(now.getTime()));
                DBObject item0 = null;
                DBObject item1 = null;
                Double acpower0 = (acpower == null) ? 0.0 : acpower;
                Double acvoltage0 = (acvoltage == null) ? 0.0 : acvoltage;
                Double acenergy_rec0 = energ;
                Double irradiance0 = 0.0;
                try (DBCursor cur0 = col2.find(testItem)) {
                    if (cur0.hasNext()) {
                        item1 = cur0.next();
                        acenergy_rec0 = acenergy_rec0 + Double.parseDouble((String) item1.get("acenergy_rec"));
                        irradiance0 = acenergy_rec0 + Double.parseDouble((String) item1.get("irradiance"));
                        acvoltage0 = acvoltage + Double.parseDouble((String) item1.get("acvoltage"));
                        acpower0 = acpower + Double.parseDouble((String) item1.get("acpower"));
                    }
                    cur0.close();
                }

                item0 = new BasicDBObject("farm_id", _farmId)
                        .append("system_id", _systemId)
                        .append("system_name", _systemName)
                        .append("farm_id", _farmId)
                        .append("timestamp", outFormat.format(now.getTime()))
                        .append("acpower", acpower0)
                        .append("accurrent", accurrent)
                        .append("acvoltage", acvoltage0)
                        .append("linefrequency", frequency)
                        .append("acenergy_rec", acenergy_rec0)
                        .append("irradiance", irradiance0)
                        .append("ambient_temp", null)
                        .append("system_temp", temperature);
                if (item1 != null) {
                    item0 = new BasicDBObject("farm_id", _farmId)
                            .append("acpower", acpower0)
                            .append("acvoltage", acvoltage0)
                            .append("acenergy_rec", acenergy_rec0)
                            .append("irradiance", irradiance0);
                }

                if (item1 != null) {
                    mongoBuilder2.find(item1).update(item0);
                } else {
                    mongoBuilder1.insert(item0);
                }
            }
            return 1;
        }
        return 0;
    }

    @Override
    public abstract void run();
}
