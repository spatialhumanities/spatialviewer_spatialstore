/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.i3mainz.ibr.export;

import de.i3mainz.ibr.connections.Config;
import de.i3mainz.ibr.filter.QueriesSS;
import de.i3mainz.ibr.filter.QueriesTS;
import de.i3mainz.ibr.filter.SpatialStoreConnect;
import de.i3mainz.ibr.filter.TripleStoreConnect;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import javax.ws.rs.core.Response;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Tag;

/**
 *
 * @author alexandra.mueller
 */
public class FeatureDetailPage {

    public static Response getPage(String sc, String fid) {

        File template;
        Document doc;
        try {
            template = new File(FeatureDetailPage.class.getClassLoader().getResource("detailpage_template.html").getPath());
            doc = Jsoup.parse(template, "UTF-8");
            
            doc.select("title").first().text("Feature Overview: " + fid);
            doc = addMetaInformation(doc, fid, sc);
            doc = addSpatialInformation(doc, fid);
            doc = addTripleInformation(doc, fid, sc);

            return Config.getResult(doc.toString());
        } catch (IOException | SQLException | ClassNotFoundException ex) {
            return Config.getResult(ex);
        }
    }

    private static Document addMetaInformation(Document doc, String fid, String sc) {
        Element meta = doc.getElementById("metainformation");
        String furl = Config.url + "/" + sc + "/features/" + fid;
        meta.select("img").first().attr("src", furl + ".png");
        meta.select("h1").first().text(fid);
        String viewerlink = Config.getProperty("gv_viewerjsp");
        meta.getElementById("viewerlink").attr("href", viewerlink);
        String featureviewerlink = viewerlink + "?furi=" + furl;
        //meta.select("a").first().attr("href", viewerurl);
        meta.getElementById("featureviewerlink").attr("href", featureviewerlink);
        String featureworkbenchlink = Config.getProperty("ts_workbench") + "/repositories/" + Config.getProperty("rep_id") + "/explore?resource=<" + furl + ">";
        meta.getElementById("featureworkbenchlink").attr("href", featureworkbenchlink);

        return doc;
    }

    private static Document addSpatialInformation(Document doc, String fid) throws SQLException, ClassNotFoundException {
        SpatialStoreConnect db = new SpatialStoreConnect();

        String q = String.format(QueriesSS.REPORT_FID, fid);
        JSONArray array = db.queryResultToJSONArray(db.getQueryResult(q));
        JSONObject values = (JSONObject) array.get(0);

        //String[] spatial_keys = {"fid", "type", "sc_name", "sc_place", "creator", "date_creation", "date_edits", "vps"};
        Element spatial = doc.getElementById("spatialinformation");

        Element label = spatial.getElementsByClass("label").get(0);
        Element info = spatial.getElementsByClass("information").get(0);
        spatial.empty();

        spatial.append(label.clone().text("Geometry Type").toString());
        spatial.append(info.clone().text(values.get("type").toString()).toString());

        spatial.append(label.clone().text("Spatial Context ID").toString());
        spatial.append(info.clone().text(values.get("sc_name").toString()).toString());

        spatial.append(label.clone().text("Spatial Context").toString());
        spatial.append(info.clone().text(values.get("sc_place").toString()).toString());

        spatial.append(label.clone().text("Creator").toString());
        spatial.append(info.clone().text(values.get("creator").toString()).toString());

        spatial.append(label.clone().text("Creation Date").toString());
        spatial.append(info.clone().text(values.get("date_creation").toString()).toString());

        spatial.append(label.clone().text("Dates Processed").toString());
        String edits = values.get("date_edits").toString();
        String[] editsArr = edits.substring(2, edits.length() - 2).split("\",\"");
        for (String edit : editsArr) {
            spatial.append(info.clone().text(edit).toString());
        }

        spatial.append(label.clone().text("Visible from Viewpoints").toString());
        String vps = values.get("vps").toString();
        String[] vpsArr = vps.substring(1, vps.length() - 1).split(",");
        for (String vp : vpsArr) {
            spatial.append(info.clone().text(vp).toString());
        }

        return doc;

    }

    private static Document addTripleInformation(Document doc, String fid, String sc) {
        String spo[] = {"subject", "predicate", "object"};
        
        String furl = "<" + Config.url + "/" + sc + "/features/" + fid + ">";
        String q = String.format(QueriesTS.REPORT_FID, furl);
        JSONArray triples = TripleStoreConnect.queryResultToJSONArray(TripleStoreConnect.getQueryResult(q));
        
        Element triple = doc.getElementById("tripleinformation");

        HashMap<String, Element> spoElements = new HashMap();
        for(String s : spo) {
            spoElements.put(s, triple.getElementsByClass(s).get(0));
        }

        triple.empty();
        
        Element link = new Element(Tag.valueOf("a"), "");
                
        for(int i = 0; i<triples.length(); i++) {
            JSONObject next = triples.getJSONObject(i);
            
            for (String s : spo) {
                String value = next.get(s).toString();
                if (value.startsWith("http")) {
                    link.attr("href", value);
                    link.text(value);
                    triple.append(spoElements.get(s).clone().html(link.toString()).toString());
                } else {
                    triple.append(spoElements.get(s).clone().text(value).toString());
                }
            }

            
            
            
            //triple.append(predicate.clone().text(next.get("predicate").toString()).toString());
            //triple.append(object.clone().text(next.get("object").toString()).toString());
        }
        
        return doc;
    }

}
