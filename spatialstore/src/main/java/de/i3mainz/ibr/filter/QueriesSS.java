/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.i3mainz.ibr.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * This class contains SQL queries and methods to prepair queries for the
 * SpatialStore. The query strings contain placeholders to fill in the spatial
 * context, WHERE-clauses and columns (properties).
 *
 * @author alexandra.mueller
 */
public class QueriesSS {

    /**
     * Key words for filter and properties in the SS.
     */
    public static final List<String> KEYS_FILTER = Arrays.asList("id", "vp", "creator", "type", "selected features");
    public static final List<String> KEYS_PROPERTIES = Arrays.asList("id", "vp", "lastedit", "creator", "type", "img");

    /**
     * Query to get feature ids (id) from the SS. Insert sc via String.format.
     */
    public static final String GET_IDS
            = "select distinct feature.id as id from feature "
            + "inner join spatialcontext on (IDREF_sc = spatialcontext.id) "
            + "where spatialcontext.name = '%s' "
            + "order by id ";

    /**
     * Query to get viewpoints (vp) from the SS. Insert sc via String.format.
     */
    public static final String GET_VPS
            = "select distinct viewpoint.name as vp from viewpoint "
            + "inner join spatialcontext on (IDREF_sc = spatialcontext.id) "
            + "where spatialcontext.name = '%s' "
            + "order by vp";

    /**
     * Query to get creators (creator) from the SS. Insert sc via String.format.
     */
    public static final String GET_CREATORS
            = "select distinct creator from edit "
            + "inner join feature on (IDREF_feature = feature.id) "
            + "inner join spatialcontext on (IDREF_sc = spatialcontext.id) "
            + "where spatialcontext.name = '%s' "
            + "and creator != '' "
            + "order by creator ";

    /**
     * Query to get geometry types (type) from the SS. Insert sc via
     * String.format.
     */
    public static String GET_TYPE
            = "select distinct geometrytype(geom) as type from feature "
            + "inner join spatialcontext on (IDREF_sc = spatialcontext.id) "
            + "where spatialcontext.name = '%s' "
            + "order by type";

    /**
     * String for the SS filter query. Insert via String.format:
     *
     * 1) spatialcontext 2) filter 3) property
     */
    public static final String FILTER_MASK
            = "select distinct %3$s from (" // insert required properties
            + "\n" + "select distinct f.id as id, vp.name as vp, creator, c.last_edit as lastedit, Geometrytype(geom) as type, "
            + "\n" + "'" + Filter.getRestBaseURL() + "/%1$s/features/' || f.id  || '.png' as img "
            + "\n" + "from spatialcontext as sc "
            + "\n" + "inner join feature as f on (sc.id = f.IDREF_sc) "
            + "\n" + "inner join viewpoint as vp on (sc.id = vp.IDREF_sc) "
            + "\n" + "  inner join feature_viewpoint as fvp on (vp.id = fvp.IDREF_view AND f.id = fvp.IDREF_feature) "
            + "\n" + "inner join "
            + "\n" + "  (select f.id as fid, creator, max(date) as last_edit from feature as f "
            + "\n" + "  inner join edit as e on (e.IDREF_feature = f.id) "
            + "\n" + "  group by f.id, creator"
            + "\n" + "  order by f.id) as c "
            + "\n" + "on (c.fid = f.id) "
            + "\n" + "where sc.name = '%1$s' " // insert sc
            + "\n" + "order by f.id) as x"
            + "\n" + "%2$s " // insert filter conditions
            + "\n" + ";";

    public static final String REPORT_FID
            = "select distinct	f.id as fid, Geometrytype(geom) as type, sc.name as sc_name, sc.place as sc_place, "
            + "\n" + "	e.creator as creator, e.date_creation as date_creation, e.date_edits as date_edits , vp.id as vps "
            + "\n" + "from feature as f "
            + "\n" + "join spatialcontext as sc on (f.idref_sc = sc.id) "
            + "\n" + "join (select f.id as fid, creator, min(date) as date_creation, array_agg(date) as date_edits from feature as f "
            + "\n" + "  join edit as e on (e.IDREF_feature = f.id) "
            + "\n" + "  group by f.id, creator) as e "
            + "\n" + "on (e.fid = f.id) "
            + "\n" + "join (select idref_feature as fid, array_agg(vp.name) as id from viewpoint as vp "
            + "\n" + "  join feature_viewpoint on (vp.id = idref_view) "
            + "\n" + "  group by idref_feature) as vp "
            + "\n" + "on (vp.fid = f.id) "
            + "\n" + "where f.id = %s "; // insert id

    /**
     * Returns a String for the SS query containing all filters.
     *
     * @param filter
     * @return
     */
    public static String filterFromArray(JSONArray filter) {
        JSONObject filterCompact = new JSONObject();
        for (int i = 0; i < filter.length(); i++) {
            JSONObject o = filter.getJSONObject(i);
            String key = o.keySet().toArray()[0].toString();
            if (key.equals("sparql")) {
                String query = (String) o.get(key);
                query = QueriesTS.addNamespaces(query);
                JSONArray result = TripleStoreConnect.queryResultToJSONArray(TripleStoreConnect.getQueryResult(query));
                for (int j = 0; j < result.length(); j++) {
                    JSONObject next = result.getJSONObject(j);
                    try {
                        String uri = next.getString("feature");
                        int fid = Filter.getFIDfromURI(uri);
                        if (fid > -1) {
                            filterCompact.append("id", fid);
                        }
                    } catch (Exception e) {
                    }
                }
            } else if (key.equals("selection")){
                JSONArray ids = o.getJSONArray(key);
                for(int j = 0; j< ids.length(); j++) {
                    filterCompact.append("id", ids.getInt(j));
                }
            } else if (KEYS_FILTER.contains(key)) {
                filterCompact.append(key, o.get(key));
            }
        }

        String f = "";
        for (Iterator it = filterCompact.keys(); it.hasNext();) {
            String key = (String) it.next();
            JSONArray valueArr = (JSONArray) filterCompact.get(key);
            String nextFilter = "";
            for (int i = 0; i < valueArr.length(); i++) {
                Object value = valueArr.get(i);
                if (value instanceof String) {
                    if (key.equals("type")) {
                        nextFilter += " OR " + key + " = 'MULTI" + value.toString() + "'";
                    }
                    value = "'" + value.toString() + "'";
                }
                nextFilter += " OR " + key + " = " + value.toString();
            }
            f += " AND (" + nextFilter.substring(3) + ") ";

        }

        return f;
    }

    /**
     * Returns a String for the SS query containing all properties.
     *
     * @param properties
     * @return
     */
    public static String propertiesFromList(ArrayList<String> properties) {
        String p = "";
        for (String nextP : properties) {
            if (KEYS_PROPERTIES.contains(nextP)) {
                p += ", " + nextP;
            }
        }
        if (!p.equals("")) {
            p = p.substring(2);
        }

        return p;
    }
    
    public static final String METADATA
            = "select one.id, one.place, date, coordsystem, count_fid, count_vp, t.param as transfromvp,  transfromsc from ( "
            + "  select sc.id, place, date, count_fid, count_vp, t1.param as transfromsc,  t1.dstCRS as coordsystem from spatialcontext as sc "
            + "  join ( "
            + "  select idref_sc, count(id) as count_fid from feature "
            + "  group by idref_sc "
            + "  ) as f on f.idref_sc = sc.id "
            + "  join ( "
            + "  select idref_sc, count(id) as count_vp from viewpoint "
            + "  group by idref_sc "
            + "  ) as vp on vp.idref_sc = sc.id "
            + "  join transformation t1 on (sc.idref_trans = t1.id) "
            + "  where sc.name = '%s' "
            + ") as one "
            + "join viewpoint vp on one.id = vp.idref_sc "
            + "join transformation t on vp.idref_trans = t.id "
            + "order by vp.id "
            + "limit 1 ";

}
