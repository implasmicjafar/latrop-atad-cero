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
 * This class is the main coordinator for all sites. It manages the data
 * acquisition
 */
public class DataGrabberCore implements Runnable{ 
    private boolean _running = false;
    private Thread _thread;
    private int _logInterval = 1800;
    private ModelSite[] _models;
    private AmazonDynamoDBClient _dbClient;
    
    public DataGrabberCore(){
        // Basic initialization
        _running = false;
        _thread = null;
    }
    
    protected boolean IsRunning() {
        return _running;
    }
    
    public void Start(){
        if(!_running){
            _thread = new Thread(this);
            _running = true;
            _thread.start();
        }
    }
    
    public void Stop(){
        if (_running) {
            _running = false;
        }
        if(_models != null){
            for(int i = 0; i < _models.length; i++){
                if((_models[i] != null) && (_models[i].IsRunning())){
                    _models[i].Stop();
                }
            }
        }
        _thread = null;
    }

    @Override
    public void run() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
