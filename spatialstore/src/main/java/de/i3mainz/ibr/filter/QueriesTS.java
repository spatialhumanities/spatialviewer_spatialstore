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
 * This class contains SPARQL queries and methods to prepair queries. The query
 * strings contain placeholders to fill in the spatial context, filters in the
 * WHERE-clause and columns (properties).
 *
 * @author alexandra.mueller
 */
public class QueriesTS {

    /**
     * Key words for filter and properties in the TS.
     */
    public static final List<String> KEYS_VALUESFOR = Arrays.asList("concept", "instance", "predicate");
    public static final List<String> KEYS_FILTER = Arrays.asList("subject", "predicate", "object");
    public static final List<String> KEYS_PROPERTIES = Arrays.asList(
            "subject", "predicate", "object", "subLabel", "predLabel", "objLabel",
            "subType", "objType", "subNote", "objNote");

    /**
     * Names spaces for the SPARQL queries.
     */
    private static final String NS_SKOS = " <http://www.w3.org/2004/02/skos/core#> ";
    private static final String NS_RDFS = " <http://www.w3.org/2000/01/rdf-schema#> ";
    private static final String NS_RDF = " <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ";

    private static final String NS_SH = " <http://ibr.spatialhumanities.de/vocab#> ";
    private static final String NS_LS = " <http://labeling.i3mainz.hs-mainz.de/vocab#> ";

    /**
     * Query to get concepts from the TS. Insert sc via String.format.
     */
    public static final String GET_CONCEPTS
            = "prefix skos:" + NS_SKOS
            + "prefix sh:" + NS_SH
            + "SELECT ?uri ?label where {"
            + "%1$s sh:hasConceptScheme ?scheme. " // sc einsetzen
            + "?uri skos:inScheme ?scheme. "
            + "?uri skos:prefLabel ?label. } "
            + "order by ?label ";

    /**
     * Query to get instances from the TS. Insert sc via String.format.
     */
    public static final String GET_INSTANCES
            = "prefix skos:" + NS_SKOS
            + "prefix sh:" + NS_SH
            + "SELECT ?uri ?label where { "
            + "%1$s sh:hasConceptScheme ?scheme. " // sc einsetzen
            + "?concept skos:inScheme ?scheme. "
            + "?uri rdf:type ?concept. "
            + "OPTIONAL{ ?uri skos:prefLabel ?label. } "
            + "OPTIONAL { BIND(?uri as ?label) FILTER NOT EXISTS {?uri skos:prefLabel ?l }}"
            + " } "
            + "order by ?label ";

    /**
     * Query to get predicates from the TS. Insert sc via String.format.
     */
    public static final String GET_PREDICATES
            = "prefix ls:" + NS_LS
            + "prefix sh:" + NS_SH
            + "prefix rdfs:" + NS_RDFS
            + "SELECT ?uri ?label where { "
            + "%1$s sh:hasPropertyScheme ?scheme. " // sc einsetzen
            + "?uri ls:inPropertyScheme ?scheme. "
            + "?uri rdfs:label ?label. } "
            + "order by ?label ";

    /**
     * String for the TS filter query. Insert via String.format:
     *
     * 1) spatialcontext 2) filter 3) property
     */
    public static final String FILTER_MASK
            = "PREFIX rdfs:" + NS_RDFS
            + "\n" + "PREFIX rdf:" + NS_RDF
            + "\n" + "prefix skos:" + NS_SKOS
            + "\n" + "PREFIX sh:" + NS_SH
            + "\n" + "PREFIX ls:" + NS_LS
            + "\n" + "PREFIX features: <%1$s/features/>" // insert sc (1)
            + "\n" + "SELECT DISTINCT ?subject ?predicate ?object ?subLabel ?predLabel ?objLabel %3$s WHERE { " // insert properties (3)
            + "\n" + "{"
            + "\n" + "   ?subject ?predicate ?object. "
            + "\n" + "   FILTER(STRSTARTS(STR(?subject), STR(features:) )). "
            + "\n" + "   BIND(\"Feature\" AS ?subType)."
            + "\n" + "} UNION { "
            + "\n" + "   ?subject ?predicate ?object. "
            + "\n" + "   <%1$s> sh:hasConceptScheme ?scheme. " // insert sc (1)
            + "\n" + "   {"
            + "\n" + "      ?concept skos:inScheme ?scheme. "
            + "\n" + "      ?subject rdf:type ?concept. "
            + "\n" + "   } UNION {"
            + "\n" + "      ?subject skos:inScheme ?scheme."
            + "\n" + "   }"
            + "\n" + "} "
            + "\n" + "{"
            + "\n" + "   ?subject ?predicate ?object. "
            + "\n" + "   FILTER(STRSTARTS(STR(?object), STR(features:) )). "
            + "\n" + "	  BIND(\"Feature\" AS ?objType)."
            + "\n" + "} UNION { "
            + "\n" + "   ?subject ?predicate ?object. "
            + "\n" + "   <%1$s> sh:hasConceptScheme ?scheme. " // insert sc (1)
            + "\n" + "   {"
            + "\n" + "      ?concept skos:inScheme ?scheme. "
            + "\n" + "      ?object rdf:type ?concept. "
            + "\n" + "   } UNION {"
            + "\n" + "      ?object skos:inScheme ?scheme."
            + "\n" + "   }"
            + "\n" + "} "
            + "\n" + "<%1$s> sh:hasPropertyScheme ?propScheme. "
            + "\n" + "?predicate  ls:inPropertyScheme ?propScheme. "
            + "\n" + "?propScheme rdf:type ls:PropertyScheme. "
            + "\n" + "OPTIONAL { BIND(STRAFTER(STR(?subject), STR(features:)) AS ?subLabel). FILTER(STRSTARTS(STR(?subject), STR(features:) )). FILTER NOT EXISTS {?subject skos:prefLabel ?fls}.} "
            + "\n" + "OPTIONAL { BIND(?subject as ?subLabel). FILTER(!STRSTARTS(STR(?subject), STR(features:) )). FILTER NOT EXISTS {?subject skos:prefLabel ?ls}. } "
            + "\n" + "OPTIONAL { ?subject skos:prefLabel ?subLabel. } "
            + "\n" + "OPTIONAL { ?predicate rdfs:label ?predLabel. } "
            + "\n" + "OPTIONAL { BIND(STRAFTER(STR(?object), STR(features:)) AS ?objLabel). FILTER(STRSTARTS(STR(?object), STR(features:) )). FILTER NOT EXISTS {?object skos:prefLabel ?flo}.} "
            + "\n" + "OPTIONAL { BIND(?object as ?objLabel). FILTER(!STRSTARTS(STR(?object), STR(features:) )). FILTER NOT EXISTS {?object skos:prefLabel ?lo}. } "
            + "\n" + "OPTIONAL { ?object skos:prefLabel ?objLabel. }"
            + "\n" + "OPTIONAL { ?subject rdf:type ?subTypeURI. ?subTypeURI rdf:type skos:Concept. ?subTypeURI skos:prefLabel ?subType. } "
            + "\n" + "OPTIONAL { ?object rdf:type ?objTypeURI.	?objTypeURI rdf:type skos:Concept. ?objTypeURI skos:prefLabel ?objType. } "
            + "\n" + "OPTIONAL { ?subject skos:note ?subNote. } "
            + "\n" + "OPTIONAL { ?object skos:note ?objNote. } "
            + "\n" + "%2$s" // insert filter (2)
            + "\n" + "} ";

    public static String REPORT_FID
            = "select * where { "
            + "\n" + "  { ?subject ?predicate %1$s. "
            + "\n" + "    BIND(%1$s as ?object) "
            + "\n" + "  } UNION { "
            + "\n" + "    %1$s ?predicate ?object. "
            + "\n" + "    BIND(%1$s as ?subject) "
            + "\n" + "  } "
            + "\n" + "} ";

    /**
     * Returns a String for the TS query containing all filters.
     *
     * @param filter
     * @return
     */
    public static String filterFromArray(JSONArray filter) {
        JSONObject filterCompact = new JSONObject();
        for (int i = 0; i < filter.length(); i++) {
            JSONObject o = filter.getJSONObject(i);
            String key = o.keySet().toArray()[0].toString();
            if (KEYS_FILTER.contains(key)) {
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
                value = "<" + value.toString() + ">";
                nextFilter += ", " + value.toString();
            }
            f += " FILTER(?" + key + " IN (" + nextFilter.substring(2) + ")). ";
        }

        return f;
    }

    /**
     * Returns a String for the TS query containing all properties.
     *
     * @param properties
     * @return
     */
    public static String propertiesFromList(ArrayList<String> properties) {

        String p = "";
        for (String nextP : properties) {
            if (KEYS_PROPERTIES.contains(nextP)) {
                p += " ?" + nextP;
            }
        }
        return p;
    }
    
    /**
     * Returns the same query but with namespace prefixes.
     * @param query
     * @return 
     */
    public static String addNamespaces(String query) {
        query = "PREFIX rdfs:" + NS_RDFS
                + "\n" + "PREFIX rdf:" + NS_RDF
                + "\n" + "prefix skos:" + NS_SKOS
                + "\n" + "PREFIX sh:" + NS_SH
                + "\n" + "PREFIX ls:" + NS_LS
                + "\n" + "PREFIX rest:" + Filter.getRestBaseURL()
                + "\n" + query;
        return query;
    }
}
