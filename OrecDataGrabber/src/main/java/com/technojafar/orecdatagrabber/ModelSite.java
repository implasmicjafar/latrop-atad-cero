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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

/**
 *
 * @author Jafaru Mohammed
 * @version 1.0
 * 2015-05-20
 * This class is intended to be inherited be other classes that perform specifics
 * as per the site technology. It is a site model.
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
    protected AmazonDynamoDBClient _dbClient;
    protected long _delay = 0;
    protected String _startTime = "";
    protected String _name = "";
    protected long _rundelay = 0;
    
    /**
     * Constructor for Model Sites without API key.
     * @param tablePrefix
     * @param systemId
     * @param systemName
     * @param username
     * @param password
     * @param farmId
     * @param dbClient 
     */
    public ModelSite(String tablePrefix, String systemId, String systemName, String username, String password, int farmId,AmazonDynamoDBClient dbClient){
       _thread = null;
       _running = false;
       
        _systemId = systemId;
        _systemName = systemName;
        _username = username;
        _password = password;
        _farmId = farmId;
        _dbClient = dbClient;
        
        _delay = 0;
        _startTime = "";
        _name = "";
        _rundelay = 0;
    } 
    
    /**
     * Constructor for Model site with API key.
     * @param tablePrefix
     * @param systemId
     * @param systemName
     * @param username
     * @param password
     * @param farmId
     * @param apiKey
     * @param dbClient 
     */
    public ModelSite(String tablePrefix, String systemId, String systemName, String username, String password, int farmId, String apiKey,AmazonDynamoDBClient dbClient) {
        _systemId = systemId;
        _systemName = systemName;
        _username = username;
        _password = password;
        _farmId = farmId;
        _apiKey = apiKey;
        _dbClient = dbClient;
        
        _thread = null;
        _running = false;
        
        _delay = 0;
        _startTime = "";
        _name = "";
        _rundelay = 0;
    }
    
    /**
     * Getter for the running state of the thread
     * @return Boolean whether the thread is running
     */
    protected boolean IsRunning() {
        return _running;
    }
    
    /**
     * Method to start the thread
     */
    protected void Start(){
        if (!_running) {
            _thread = new Thread(this);
            _running = true;
            _delay = 0;
            _thread.start();
        }
    }
    
    /**
     * Method to stop the thread
     */
    protected void Stop(){
        if (_running) {
            _running = false;
            /*
            DatabaseManMysql db = new DatabaseManMysql(_dbPort, _dbHost, _dbName, _dbUser, _dbPassw);
            db.cook();
            try {
                CallableStatement cs = db.conn.prepareCall("call orec_put_last_timestamp(?)");
                cs.setInt(1, _farmId);
                cs.execute();
                db.conn.close();
            } catch (SQLException ex) {
                try {
                    db.conn.close();
                } catch (SQLException ex1) {
                    Logger.getLogger(Model.class.getName()).log(Level.SEVERE, null, ex1);
                }
                Logger.getLogger(Model.class.getName()).log(Level.SEVERE, null, ex);
            }
                    */
            _running = false;
        }
        _thread = null;
    }
    
    @Override
    public abstract void run();
}
