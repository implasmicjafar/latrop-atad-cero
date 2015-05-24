/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.technojafar.orecdatagrabber;

import com.mongodb.DB;

/**
 *
 * @author T3ee
 */
public class IrradianceModelSite extends ModelSite{

    public IrradianceModelSite(String tablePrefix, int systemId, String systemName, String username, String password, int farmId, String apiKey, DB db) {
        super(tablePrefix, "", systemName, username, password, farmId, apiKey, db);
    }

    @Override
    public void run() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
