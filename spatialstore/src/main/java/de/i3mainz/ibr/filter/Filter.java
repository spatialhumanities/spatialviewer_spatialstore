/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.i3mainz.ibr.filter;

import de.i3mainz.ibr.connections.Config;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.json.JSONObject;
import javax.ws.rs.core.Response;
import org.json.JSONArray;
import org.openrdf.query.BindingSet;

/**
 * Provides methods to answer client queries for filtering. Decides if
 * TripleStore or SpatialStore has to be queried. For filters the TripleStore is
 * queried first, additional properties can then be queried from the
 * SpatialStore.
 */
public class Filter {
    private static String EXCEPTION_STRING = "";
    // json keywords
    public final static String KEY_VALUESFOR = "valuesfor";
    public final static String KEY_FILTER = "filter";
    public final static String KEY_PROPERTIES = "properties";

    public static String getRestBaseURL() {
        try {
            return Config.getProperty("restbaseurl");
        } catch (NullPointerException ex) {
            return Config.getProperty("gv_rest");
        }

    }

    /**
     * Manages the further process depending on the input key and values of the
     * json.
     *
     * @param sc
     * @param json_str
     * @return
     */
    public static Response filter(String sc, String json_str) {
        JSONObject json = new JSONObject(json_str);
        if (json.has(KEY_VALUESFOR)) {
            String value = json.getString(KEY_VALUESFOR);
            if (QueriesTS.KEYS_VALUESFOR.contains(value)) {
                return TripleStoreConnect.getFilterValues(sc, value);
            } else if (QueriesSS.KEYS_FILTER.contains(value)) {
                return SpatialStoreConnect.getFilterValues(sc, value);
            }
        } else if (json.has(KEY_PROPERTIES)) {
            ArrayList<String> properties = new ArrayList<>();
            JSONArray jsonArray = (JSONArray) json.get(KEY_PROPERTIES);
            for (int i = 0; i < jsonArray.length(); i++) {
                properties.add(jsonArray.getString(i));
            }

            JSONArray filter = new JSONArray();
            if (json.has(KEY_FILTER)) {
                filter = json.getJSONArray(KEY_FILTER);
            }
            return getFilterResult(sc, filter, properties);
        }

        return Config.getResult(new Exception("No valid key found."));
    }

    /**
     * Reads filter from JSONArray and builds queries with filter and properties
     * for TripleStore and SpatialStore. Returns response with result of the
     * filtering as json.
     *
     * @param sc
     * @param filter
     * @param properties
     * @return
     */
    private static Response getFilterResult(String sc, JSONArray filter, ArrayList<String> properties) {
        HashMap map = new HashMap();
        String fTS = "", pTS;

        String ns_sc = getRestBaseURL() + "/" + sc;

        // 1) Filter und Properties fÃƒÂ¼r Query an TS auslesen und sammeln
        fTS = QueriesTS.filterFromArray(filter);
        pTS = QueriesTS.propertiesFromList(properties);
        
        if (!(fTS.equals("") && pTS.equals(""))) {
            // 2) Query um Filter und Properties ergÃƒÂ¤nzen, an TS senden
            String qTS = String.format(QueriesTS.FILTER_MASK, ns_sc, fTS, pTS);
            List<BindingSet> tsResult = TripleStoreConnect.getQueryResult(qTS);
            JSONArray arr = TripleStoreConnect.queryResultToJSONArray(tsResult);

            // 3) Triples nach FID sammeln
            try {
                map = collectByFID(arr);
            } catch (NumberFormatException ex) {
                return Config.getResult(new Exception(EXCEPTION_STRING));
            }
        }
        // 4) FIDs, Filter und Properties fÃƒÂ¼r Query an SS auslesen und sammeln
        Set<Integer> fids = map.keySet();
        String fSS = "";
        if (!fTS.equals("")) {
            String s = fids.toString().replaceAll("\\[", "").replaceAll("\\]", "");
            fSS = " AND (id = ANY('{" + s + "}'::int[]))";
        }

        // create filter and property strings for SS query
        fSS += QueriesSS.filterFromArray(filter);

        if (!fSS.equals("")) { // erstes AND ersetzen durch WHERE
            fSS = " WHERE " + fSS.substring(5);
        }
        String pSS = QueriesSS.propertiesFromList(properties);

        // 5) Filter und Properties in Query einsetzen, an SS senden
        String qSS = String.format(QueriesSS.FILTER_MASK, sc, fSS, pSS);
        try (SpatialStoreConnect db = new SpatialStoreConnect()) {
            ResultSet rs = db.getQueryResult(qSS);
            JSONArray array = db.queryResultToJSONArray(rs);

            // 6) Filterergebnisse zusammenfuegen (merge ueber FIDs)
            // Rueckgabe enthaelt ein JSONObject im JSONArray pro Tabellenzeile
            return Config.getResult(merge(array, map).toString());

        } catch (SQLException | ClassNotFoundException ex) {
            return Config.getResult(ex);
        }
    }

    /**
     * Uses the feature IDs (=key) and collects all JSON Objects in a list
     * (=value), that contain the feature in the triple.
     *
     * @param arr
     * @return
     */
    private static HashMap collectByFID(JSONArray arr) throws NumberFormatException {
        HashMap<Integer, List<JSONObject>> map = new HashMap();
        List<JSONObject> l = new ArrayList<>();
        map.put(-1, l);

        for (int i = 0; i < arr.length(); i++) {
            JSONObject o = arr.getJSONObject(i);
            boolean used = false;
            for (Object key : o.keySet()) {
                String val = o.get(key.toString()).toString();
                if (val.contains(getRestBaseURL()) && val.contains("features")) {
                    used = true;
                    int fid = getFIDfromURI(val);
                    
                    if (fid == -1) {
                        throw new NumberFormatException();
                    }
                    
                    if (map.containsKey(fid)) {
                        map.get(fid).add(o);
                    } else {
                        l = new ArrayList<>();
                        l.add(o);
                        map.put(fid, l);
                    }
                }
            }
            if (!used) {
                map.get(-1).add(o);

            }
        }
        return map;
    }
    
    /**
     * Extracts feature id (fid) from the given feature uri (uri).
     * Return -1 if: <br>
     *  - URI is no feature URI
     *  - FID cannot be found
     *  - FID cannot be parsed to Integer.
     * 
     * @param uri
     * @return
     * @throws NumberFormatException 
     */
    public static Integer getFIDfromURI(String uri) {
        int fid = -1;
        if (!(uri.contains(getRestBaseURL()) && uri.contains("features"))) {
            return fid;
        }
        
        int lastIndex = uri.lastIndexOf("/");
        if (lastIndex == uri.length() - 1) {
            uri = uri.substring(0, uri.length() - 1);
            lastIndex = uri.lastIndexOf("/");
        }
        EXCEPTION_STRING = uri + " lastIndex: " + lastIndex;
        
        try {
            fid = Integer.parseInt(uri.substring(lastIndex + 1));
        } catch (NumberFormatException ex) {
            fid = -1;
        }
        
        return fid;

    }

    /**
     * Merges the map containing lists of JSONObjects (=value) for feature IDs
     * (=key) into the JSONArray. JSONArray and HashMap are merged by the
     * feature IDs. The HashMap contains data of the TripleStore collected and
     * stacked by the feature ID.
     *
     * Output: JSOONObject with key "features" and JSONArray as value. The
     * JSONArray contains one element (JSONObject) per line in the table.
     *
     *
     * @param arr
     * @param map
     * @return
     */
    private static JSONObject merge(JSONArray arr, HashMap<Integer, List<JSONObject>> map) {
        JSONArray outarr = new JSONArray();
        for (int i = 0; i < arr.length(); i++) {
            JSONObject ss = (JSONObject) arr.get(i);
            int fid = ss.getInt("id");
            if (map.containsKey(fid)) {
                for (JSONObject ts : map.get(fid)) { // for each json of one fid
                    outarr.put(merge(ss, ts));
                }
            } else {
                outarr.put(ss);
            }
        }
        if (map.containsKey(-1)) {
            for (JSONObject o : map.get(-1)) {
                outarr.put(o);
            }
        }

        JSONObject out = new JSONObject();
        out.put("features", outarr);
        return out;
    }

    /**
     * Merges two JSON Objects into one. If both contain the same key, the value
     * of the second JSON Object will be put.
     *
     * @param one
     * @param two
     * @return
     */
    private static JSONObject merge(JSONObject one, JSONObject two) {
        JSONObject merged = new JSONObject();
        for (String k : (Set<String>) one.keySet()) {
            merged.put(k, one.get(k));
        }
        for (String k : (Set<String>) two.keySet()) {
            merged.put(k, two.get(k));
        }
        return merged;
    }

    
}
