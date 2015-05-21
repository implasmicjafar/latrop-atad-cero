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

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.technojafar.Common.LocateClass;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ini4j.Ini;

/**
 *
 * @author Jafaru Mohammed
 * @version 1.0 2015-05-20 This class is the main coordinator for all sites. It
 * manages the data acquisition
 */
public class DataGrabberCore implements Runnable {

    private boolean _running = false;
    private Thread _thread;
    private MongoClient _dbClient;
    private DB _mongoDb;
    private ModelSite[] _models;
    private String _fileLoc = null;
    private String _mongo_db_host = null;
    private String _mongo_db_port = null;
    private String _mongo_db_database = null;
    private String _table_prefix = null;
    private int _logInterval = 1800;

    public DataGrabberCore(String fileLoc) {
        // Basic initialization
        _running = false;
        _thread = null;
        _fileLoc = fileLoc;
    }

    protected boolean IsRunning() {
        return _running;
    }

    public void Start() {
        if (!_running) {
            _thread = new Thread(this);
            _running = true;
            _thread.start();
        }
    }

    public void Stop() {
        if (_running) {
            _running = false;
        }
        if (_models != null) {
            for (int i = 0; i < _models.length; i++) {
                if ((_models[i] != null) && (_models[i].IsRunning())) {
                    _models[i].Stop();
                }
            }
        }
        _thread = null;
    }

    @Override
    public void run() {
        if (_running && loadSettings() && (_thread != null) && (_fileLoc != null) && (_mongo_db_host != null)
                && (_mongo_db_port != null) && (_mongo_db_database != null) && (_table_prefix != null) && (!_mongo_db_host.isEmpty())
                && (!_mongo_db_port.isEmpty()) && (!_mongo_db_database.isEmpty())) {
            try {
                // Everything checked out ok, prepare the databaseclient
                _dbClient = new MongoClient( _mongo_db_host , Integer.parseInt(_mongo_db_port.trim()));                
            } catch (UnknownHostException ex) {
                Logger.getLogger(DataGrabberCore.class.getName()).log(Level.SEVERE, null, ex);
                System.out.println("Failed to initiate the database on host: "+ _mongo_db_host + " and port: " + _mongo_db_port.trim());
                Stop();
                return;
            }
            _mongoDb = _dbClient.getDB(_mongo_db_database);
            
        } else {
            Stop();
        }
    }

    private boolean loadSettings() {
        // The settings file is stored close to the application
        try {
            if (_fileLoc == null) {
                _fileLoc = LocateClass.getClassLocation(this.getClass()).getParent();
                File f = new File(_fileLoc);
                _fileLoc = f.getParent() + File.separator + "config";
                f = new File(_fileLoc);
                if (!f.exists()) {
                    try {
                        f.mkdirs();
                        _fileLoc = _fileLoc + File.separator + "settings.conf";
                    } catch (Exception ex) {
                        _fileLoc = null;
                        System.err.println("Failed to create configuration folder");
                        return false;
                    }
                }
            }
            File file = new File(_fileLoc);
            if (!file.exists()) {
                file.createNewFile();
            }
            Ini settings = new Ini(file);
            if (settings.isEmpty()) {
                settings.clear();

                // Set AWS config
                settings.put("Database", "host", "127.0.0.1");
                settings.put("Database", "port", "27017");
                settings.put("Database", "database", "orec");
                settings.put("Database", "table_prefix", "");

                // Set Settings
                settings.put("Settings", "LogInterval", "1800"); // Seconds

                settings.store();
                System.out.println("Configuration file not found or empty. A new template was created at\n" + _fileLoc);
                
                _mongo_db_host = "127.0.0.1";
                _mongo_db_port = "27017";
                _mongo_db_database = "orec";
                _table_prefix = "";
                
                return true;

                //System.exit(0);
            }
            // Load default
            _mongo_db_host = settings.get("Database", "host");
            _mongo_db_port = settings.get("Database", "port");
            _mongo_db_database = settings.get("Database", "orec");
            _table_prefix = settings.get("Database", "table_prefix");

            // Load settings
            _logInterval = Integer.parseInt(settings.get("Settings", "LogInterval").trim());

           // Load Sites
           /*
             String farms = settings.get("Sites", "Farms");
             if((farms == null) || (farms.isEmpty())){
             _sites = new SolarFarm[0];
             }else{
             String[] sites = farms.trim().split(",");
             _sites = new SolarFarm[sites.length];
             for(int i = 0; i < sites.length; i++){                 
             _sites[i].id = Integer.parseInt(settings.get(sites[i], "Id"));
             _sites[i].system_type = settings.get(sites[i], "SystemType");
             _sites[i].system_id = settings.get(sites[i], "SystemId");
             _sites[i].system_name = settings.get(sites[i], "SystemName");
             _sites[i].username = settings.get(sites[i], "Username");
             _sites[i].password = settings.get(sites[i], "Password");
             _sites[i].rundelay = Integer.parseInt(settings.get(sites[i], "Delay"));
             }
             }
             */
            return true;
        } catch (IOException ex) {
            Logger.getLogger(DataGrabberCore.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }

    class SolarFarm {

        public int id = -1;
        public String system_type = "";
        public String system_id = "";
        public String system_name = "";
        public String username = "";
        public String password = "";
        public String api_key = null;
        public String start = "";
        public long rundelay = 184200000;
    }

    class IrradianceSource {

        public int id_u = -1;
        public int id = -1;
        public String description = "";
        public String location = "";
        public String unit = "";
        public String url = "";
        public String start = "";
        public long rundelay = 184200000;
    }
}
