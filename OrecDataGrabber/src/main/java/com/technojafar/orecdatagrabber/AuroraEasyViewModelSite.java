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
public class AuroraEasyViewModelSite extends ModelSite {

    public AuroraEasyViewModelSite(String tablePrefix, String systemId, String systemName, String username, String password, int farmId, DB db) {
        super(tablePrefix, systemId, systemName, username, password, farmId, db);
    }

    @Override
    public void run() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
