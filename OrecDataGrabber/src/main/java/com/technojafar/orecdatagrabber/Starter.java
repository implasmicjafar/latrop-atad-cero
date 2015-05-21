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

/**
 *
 * @author Jafaru Mohammed
 * @version 1.0
 * 2015-05-20
 * This is the startup class and the only class with a static main function
 */
public class Starter {
    // Singular instance object
    static Starter starter;
    
    public Starter(String configFile){
        //This thread only waits, all other work occurs on another thread
        final DataGrabberCore dataGrabberCore = new DataGrabberCore(configFile);
        dataGrabberCore.Start(); // As a threaded class, start running
        // However, when the application shuts, gracefully shutdown the core
        // too.
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                if((dataGrabberCore != null) && dataGrabberCore.IsRunning()){
                    dataGrabberCore.Stop();
                } 
            }
        }); 
        
    }
    
    public static void main(String[] args){
        // Just instantiate only
        if (args.length > 0){
            starter = new Starter(args[0]);
        }else{
            starter = new Starter(null);        
        }
    }
}
