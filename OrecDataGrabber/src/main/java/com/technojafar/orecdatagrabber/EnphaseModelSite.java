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
 * This class is intended to handle the extraction of Data from enphase sites.
 */
public class EnphaseModelSite extends ModelSite{

    public EnphaseModelSite(String tablePrefix, String systemId, String systemName, String username, String password, int farmId, String apiKey, AmazonDynamoDBClient dbClient) {
        super(tablePrefix, systemId, systemName, username, password, farmId, apiKey, dbClient);
    }

    @Override
    public void run() {
        
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
