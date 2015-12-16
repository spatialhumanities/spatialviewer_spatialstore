/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.i3mainz.ibr.filter;

import de.i3mainz.ibr.connections.Config;
import de.i3mainz.ibr.database.DBInterface;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Provides methods to query the SpatialStore. Input for mehtods should be list
 * of feature ids and list of required properties. Output is some kind of list
 * or hashmap.
 *
 */
public class SpatialStoreConnect extends DBInterface {

    /**
     * Constructor.
     */
    public SpatialStoreConnect() throws SQLException, ClassNotFoundException {
        super();
    }

    /**
     * Sends a query to the rS and returns the result ResultSet.
     *
     * @param query
     * @return
     */
    public ResultSet getQueryResult(String query) throws SQLException {

        ResultSet resultSet = null;
        try {
            Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            resultSet = statement.executeQuery(query);

        } catch (Exception e) {
            e.printStackTrace();
            String out = e.toString() + "from getQueryResult()";
            throw new SQLException(out);
        }
        return resultSet;
    }

    /**
     * Converts the query result (ResultSet) to JSONArray.
     *
     * @param result
     * @return
     */
    public JSONArray queryResultToJSONArray(ResultSet rs) throws SQLException {
        ResultSetMetaData metadata = rs.getMetaData();
        int colcount = metadata.getColumnCount();
        List<String> colnames = new ArrayList<>();
        for (int i = 1; i <= colcount; i++) {
            colnames.add(metadata.getColumnName(i));
        }

        JSONArray allFeatureJSONs = new JSONArray();
        while (rs.next()) {
            JSONObject nextFeature = new JSONObject();
            for (String colname : colnames) {
                Object value = rs.getObject(colname);
                if (value == null) {
                    value = "";
                }
                nextFeature.put(colname, value);
            }
            allFeatureJSONs.put(nextFeature);
        }
        return allFeatureJSONs;
    }

    /**
     * Queries the SS for possible values of a filtertype and returns JSONObject
     * with values.
     *
     * @param sc
     * @param filtertype
     * @return
     */
    public static Response getFilterValues(String sc, String filtertype) {

        String query = "";
        switch (filtertype) {
            case "id":
                query = QueriesSS.GET_IDS;
                break;
            case "vp":
                query = QueriesSS.GET_VPS;
                break;
            case "creator":
                query = QueriesSS.GET_CREATORS;
                break;
            case "type":
                query = QueriesSS.GET_TYPE;
        }
        query = String.format(query, sc);

        try (SpatialStoreConnect db = new SpatialStoreConnect()) {
            ResultSet rs = db.getQueryResult(query);
            JSONArray array = db.queryResultToJSONArray(rs);
            JSONObject out = new JSONObject();
            out.put(Filter.KEY_VALUESFOR, array);
            return Config.getResult(out.toString());

        } catch (SQLException | ClassNotFoundException ex) {
            return Config.getResult(ex);
        }

    }

}
