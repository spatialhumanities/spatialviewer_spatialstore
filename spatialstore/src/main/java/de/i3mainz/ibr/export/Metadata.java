/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.i3mainz.ibr.export;

import de.i3mainz.ibr.connections.Config;
import de.i3mainz.ibr.filter.QueriesSS;
import de.i3mainz.ibr.filter.SpatialStoreConnect;
import de.i3mainz.ibr.geometry.Point;
import de.i3mainz.ibr.geometry.Transformation;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.Response;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author alexandra.mueller
 */
public class Metadata {

    public static Response getMetadataResponse(String spatialcontext) {
        JSONObject values = querySS(spatialcontext);
        values.put("workbench", Config.getProperty("ts_workbench"));
        values.put("repository", Config.getProperty("rep_id"));


        return Config.getResult(values.toString());
        //localhost:8084/spatialstore/rest/oberwesel/metadata
    }
    
    
    private static JSONObject querySS(String spatialcontext) {
        SpatialStoreConnect db;
        JSONObject values = new JSONObject();
        try {
            db = new SpatialStoreConnect();
            String q = String.format(QueriesSS.METADATA, spatialcontext);
            JSONArray array = db.queryResultToJSONArray(db.getQueryResult(q));
            values = (JSONObject) array.get(0);

        } catch (SQLException | ClassNotFoundException ex) {
            Logger.getLogger(Metadata.class.getName()).log(Level.SEVERE, null, ex);
        }

        Transformation transfromvp = new Transformation(values.getString("transfromvp"));
        Transformation transfromsc = new Transformation(values.getString("transfromsc"));
        Point coordinate = new Point(0, 0, 0);
        coordinate = transfromvp.transform(coordinate);
        coordinate = transfromsc.transform(coordinate);
        
        String c = coordinate.getX() + " | " + coordinate.getY();
        values.put("coordinate", c);
        return values;
    }
}
