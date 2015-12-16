package de.i3mainz.ibr.filter;

import de.i3mainz.ibr.connections.ClientException;
import de.i3mainz.ibr.connections.Config;
import static de.i3mainz.ibr.filter.Filter.getRestBaseURL;
import de.i3mainz.ls.rdfutils.SesameConnect;
import de.i3mainz.ls.rdfutils.exceptions.SesameSparqlException;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.Response;
import org.json.JSONArray;
import org.json.JSONObject;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.simpleframework.http.Status;

/**
 * Provides methods to query the TripleStore.
 *
 * The queries are sent to the TripleStore via SesameConnect.java.
 */
public class TripleStoreConnect {

    private static boolean ts_init = false;
    public static String TS_URL = Config.getProperty("ts_sesame");
    public static String TS_WORKBENCH = Config.getProperty("ts_workbench");
    

    /**
     * Checks if connection is initialized.
     */
    private static void init() {
        if (!ts_init) {
            de.i3mainz.ls.Config.Config.setTripleStoreServerURL(TS_URL);
            de.i3mainz.ls.Config.Config.setTripleStoreWorkbenchURL(TS_WORKBENCH);
            ts_init = true;
        }                                                                            
    }

    /**
     * Checks if the input triple (tripleInput) exists and, if not, writes the
     * new triple to the repository.
     *
     * @param tripleInput triple as json
     * @return
     */
    public static Response annotate(String tripleInput) {
        
        init();

        JSONObject json = new JSONObject(tripleInput);
        
        String subject = json.getString("subject");
        if (!subject.startsWith("http")) {
            return createInstance(json); // prüft triple und legt neue Instanz an
        }
        
        String object = json.getString("object");
        if (!object.startsWith("http")) {
            object = "\"" + object + "\"";
        } else {
            object = "<" + object + ">";
        }

        
        String triple = "<" + subject + "> <" + json.getString("predicate") + "> " + object;

        return saveTriple(triple);
    }
    
    private static Response saveTriple(String triple) {
        String repID = Config.getProperty("rep_id");
        
        try {
            if (!checkTripleExists(repID, triple)) {
                String query = "INSERT DATA {" + triple + "}";
                SesameConnect.SPARQLupdate(repID, query);

                return Config.getResult("Triple [" + triple + "] in Repository " + repID + " geschrieben.");
            } else {
                return Config.getResult("Triple [" + triple + "] existiert bereits.");
            }

        } catch (RepositoryException | MalformedQueryException | QueryEvaluationException | SesameSparqlException | UpdateExecutionException ex) {
            Logger.getLogger(TripleStoreConnect.class.getName()).log(Level.SEVERE, null, ex);
            return Config.getResult(ex);
        }
    }
    

    
    private static Response createInstance(JSONObject json) {
        String rdftype = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        String ns = Config.getProperty("gv_rest") + "/instances/";

        //check if predicate is rdf:type ("a")
        String predicate = json.getString("predicate");
        if (!predicate.equals(rdftype)) {
            return Config.getResult(new ClientException("Predicate muss 'a' sein um neue Instanz anzulegen! ", 400));
        }
        
        // check if object is some class in the triplestore
        String object = json.getString("object");
        String query = String.format(QueriesTS.GET_CONCEPTS, "?sc");
        boolean objIsClass = false;
        JSONArray result = queryResultToJSONArray(getQueryResult(query));
        int i = 0;
        while(i<result.length() && objIsClass == false){
            String nextURI = result.getJSONObject(i).get("uri").toString();
            objIsClass = nextURI.equals(object);
            i++;
        }
        System.out.println("objIsClass = " + objIsClass );
        if(!objIsClass) {
            return Config.getResult(new ClientException("Object muss eine Class sein!", 400));
        }       
        
        
        String literal = json.getString("subject");
        String subject;

        if(literal.startsWith("http") && !literal.contains(" ")) {
            subject = literal;
        } else 
            subject = ns + literal.replaceAll(" ", "_");
        
        
        String tripleInstance = "<" + subject + "> <"  + predicate + "> <"  + object + ">";
        Response rInstance = saveTriple(tripleInstance);
        
        if(!subject.equals(literal) && rInstance.getStatus() == 200) {
            String triplePrefLabel = "<" + subject + "> <http://www.w3.org/2004/02/skos/core#prefLabel>  \"" + literal + "\"";
            return saveTriple(triplePrefLabel);
        } else return rInstance;
    }

    /**
     * Checks if a triple (triple) exists in the repository (repID).
     *
     * @param triple triple String (subject predicate object)
     * @return true/false
     *
     * @throws RepositoryException
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
     * @throws SesameSparqlException
     */
    private static boolean checkTripleExists(String repID, String triple)
            throws RepositoryException, MalformedQueryException, QueryEvaluationException, SesameSparqlException {

        String query = "SELECT (COUNT(*) AS ?count) WHERE { " + triple + "}";
        List<BindingSet> sos = SesameConnect.SPARQLquery(repID, query);
        int count = Integer.parseInt(sos.get(0).getBinding("count").getValue().stringValue());

        return count > 0;
    }

    /**
     * Reads from the TripleStore what values can be set for a Concept, Instance
     * or Predicate.
     *
     * @param sc
     * @param filtertype
     * @return
     */
    public static Response getFilterValues(String sc, String filtertype) {
        String query;
        switch (filtertype) {
            case "concept":
                query = QueriesTS.GET_CONCEPTS;
                break;
            case "instance":
                query = QueriesTS.GET_INSTANCES;
                break;
            case "predicate":
                query = QueriesTS.GET_PREDICATES;
                break;
            default:
                return Config.getResult(new Exception("Insert 'concept', 'instance' or 'predicate' as value for this key."));
        }
        
        
        query = String.format(query, "<" + Filter.getRestBaseURL() + "/" + sc + ">");
        System.out.println("QUERY: " + query);
        return TripleStoreConnect.query(query);
    }

    /**
     * Sends a query to the repository in the TS and return Response
     * with JSONObject.
     *
     * @param query
     * @return
     */
    public static Response query(String query) {
        init();
        
        if(!SesameConnect.checkQuery(Config.getProperty("rep_id"), query)) {
            return Config.getResult(new Exception("Invalid Query!"));
        }
        
        List<BindingSet> result = getQueryResult(query);

        if (result != null) {
            JSONArray jsonArr = queryResultToJSONArray(result);
            JSONObject json = new JSONObject();
            json.put("result", jsonArr);
            return Config.getResult(json.toString());

        } else {
            return Config.getResult(new Exception("Query failed or returns empty result: " + query));
        }
    }

    /**
     * Sends a query to the repository in the TS and returns the result
     * as List of BindingSets.
     *
     * @param query
     * @return
     */
    public static List<BindingSet> getQueryResult(String query) {
        init();
        
        try {
            String repID = Config.getProperty("rep_id");
            List<BindingSet> result = SesameConnect.SPARQLquery(repID, query);

            return result;

        } catch (RepositoryException | MalformedQueryException | QueryEvaluationException | SesameSparqlException ex) {
            Logger.getLogger(TripleStoreConnect.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }
    
    
    /**
     * Converts the query result (List of BindingSets) to JSONArray. 
     * 
     * @param result
     * @return 
     */
    public static JSONArray queryResultToJSONArray(List<BindingSet> result) {
        JSONArray jsonArr = new JSONArray();
        if (!result.isEmpty()) {
            Set<String> names = result.get(0).getBindingNames();

            for (BindingSet bs : result) {
                JSONObject json = new JSONObject();
                for (String name : names) {
                    Value v = bs.getValue(name);
                    if (v == null) {
                        json.put(name, "");
                    } else {
                        json.put(name, v.stringValue());
                    }
                }
                jsonArr.put(json);
            }
        }
        return jsonArr;
    }

}
