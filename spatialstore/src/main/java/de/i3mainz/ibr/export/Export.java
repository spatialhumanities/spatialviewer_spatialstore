/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.i3mainz.ibr.export;

import de.i3mainz.ibr.connections.Config;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import javax.ws.rs.core.Response;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Class to export data, that is sent to the server.
 *
 * Not for features. See package
 * {@link de.i3mainz.ibr.getfeature.Export getfeature.Export}.
 */
public class Export {

    public static Response export(String json_str) {
        JSONObject json = new JSONObject(json_str);

        String type, format;
        Object params;
        if (!(json.has("type") && json.has("format") && json.has("params"))) {
            return Config.getResult(new Exception("export requires keys 'type', 'format' and 'params'"));
        } else {
            type = (String) json.getString("type");
            format = (String) json.getString("format");
            params = json.get("params");
        }

        // was soll exportiert werden?
        switch (type) {
            case "table":
                return exportTable(format, (JSONObject) params);
            case "image":
                return exportImage(format, (String) params);
            case "report":
                return exportReport(format, (JSONObject) params);
        }

        return Config.getResult(new Exception("no export"));
    }

    //<editor-fold defaultstate="collapsed" desc="exportTable">
    /**
     * Export a table in the given format. <br>
     *
     * The JSONObject json requires the keys: <br>
     * - headers (value: JSONArray with headers)<br>
     * - content (value: JSONArray with JSONArray for each line)<br>
     *
     * @return
     */
    private static Response exportTable(String format, JSONObject json) {
        String sep;
        switch (format) {
            case "csv":
                sep = ";";
                break;
            case "tsv":
                sep = "\t";
                break;
            //case "custom":
            //    sep = "...";
            //   break;
            default:
                return Config.getResult(new Exception("Export format \"" + format + "\""
                        + " not supported."));
        }

        String file = "";
        JSONArray arr;
        arr = json.getJSONArray("headers");
        file += jsonArrayToString(arr, sep);

        arr = json.getJSONArray("content");
        file += jsonArrayToString(arr, sep);

        return Config.getResult(file, "tableExport." + format);
    }

    /**
     * Returns elements of a (n-dimensional) JSONArray as String with separator.
     * Use ";" or "," for CSV, "\t" for TSV.
     *
     * @param arr
     * @param separator
     * @return
     */
    private static String jsonArrayToString(JSONArray arr, String separator) {
        String str = "";
        for (int i = 0; i < arr.length(); i++) {
            Object o = arr.get(i);
            if (o instanceof JSONArray) {
                str += jsonArrayToString((JSONArray) o, separator);
            } else if (i == arr.length() - 1) {
                str += o.toString();
            } else {
                str += o.toString() + separator;
            }
        }
        return str + "\n";
    }

    //</editor-fold>
    
    //<editor-fold defaultstate="collapsed" desc="exportImage">
    private static Response exportImage(String format, String input_str) {
        String img_str = input_str.substring(input_str.indexOf(",") + 1);
        
        try (InputStream stream = new ByteArrayInputStream(Base64.decodeBase64(img_str.getBytes()))) {
            File fileOut = new File("ImageExport." + format);
            OutputStream out = new FileOutputStream(fileOut);

            byte[] bytes = new byte[stream.available()];
            out.write(bytes);
            
            return Config.getResult(fileOut, "ImageExport." + format);
        } catch (IOException ex) {
            return Config.getResult(ex);
        }
        
    }

    //</editor-fold>
    
    //<editor-fold defaultstate="collapsed" desc="exportReport">
    private static Response exportReport(String format, JSONObject json) {
        linecounter = 0;
        String template = "";
        try {
            FileInputStream fis = new FileInputStream(FeatureDetailPage.class.getClassLoader().getResource("report_template.txt").getPath());
            template = IOUtils.toString(fis);
        } catch (IOException ex) {
            return Config.getResult(new Exception("Cannot read template!"));
        }
        
        DateFormat df = new SimpleDateFormat("yyy-MM-dd_HH-mm-ss");
        Calendar cal = Calendar.getInstance();
        String date = df.format(cal.getTime());
        
        template = template.replace("%date%", date);
        try {
            template = template.replace("%user%", json.getString("user"));
        } catch (Exception e) {
            template = template.replace("%user%", "[not logged in]");
        }
        
        String viewerURL = json.getString("url");
        viewerURL = viewerURL.lastIndexOf("#") == viewerURL.length()-1 ? viewerURL.substring(0, viewerURL.length()-1) : viewerURL;
        template = template.replace("%url%", viewerURL);
        
        template = template.replace("%scname%", json.getString("scname"));
        template = template.replace("%scuri%", json.getString("scuri"));
                
        template = template.replace("%filter%", getFilter(json.getJSONArray("filter")));
        // properties
        template = template.replace("%tablecolumns%", getProperties(json.getJSONArray("tablecolumns")));
        
        // tablecontent
        String featureNS = json.get("featurens").toString();
        template = template.replace("%tablecontent%", getTableContent(json.getJSONArray("tablecontent"), json.getJSONArray("tablecolumns"), featureNS, viewerURL));
        template = template.replace("%linecounter%", Integer.toString(linecounter));

        String filename = "report_" + date + "." + format;

        return Config.getResult(template, filename);
    }
    
    private static String getFilter(JSONArray filter) {
        String f = "";
        
        JSONObject filterCompact = new JSONObject();
        for (int i = 0; i < filter.length(); i++) {
            JSONObject o = filter.getJSONObject(i);
            String key = o.keySet().toArray()[0].toString();
            filterCompact.append(key, o.get(key));
        }
        
        for (Iterator it = filterCompact.keys(); it.hasNext();) {
            String key = (String) it.next();
            JSONArray valueArr = (JSONArray) filterCompact.get(key);
            String nextFilter = "";
            for (int i = 0; i < valueArr.length(); i++) {
                String value = valueArr.get(i).toString();
                nextFilter += "\n\t\t" + value;
            }
            f += "\n\t" + key + nextFilter;
        }
        f = f.equals("") ? "[no filters set]" : f;
        return f;
    }
    
    private static String getProperties(JSONArray properties) {
        String allP = "";
        for (int i = 0; i < properties.length(); i++) {
            String nextP = properties.getString(i);
            allP += "\n\t" + nextP;
        }
        
        return allP;
    }
    
    private static int linecounter = 0;
    private static String getTableContent(JSONArray tablecontent, JSONArray tableheaders, String featureNS, String viewerURL) {
        
        // add 2 column headers
        tableheaders.put("draw (show feature in viewer)");
        tableheaders.put("Feature URI (show detail page)");
        
        // find maximum column widths
        int maxColLength[] = new int[tableheaders.length()];
        for (int i = 0; i < tableheaders.length(); i++) {
            maxColLength[i] = tableheaders.get(i).toString().length();
        }
        for (int i = 0; i < tablecontent.length(); i++) {
            JSONArray nextLine = tablecontent.getJSONArray(i);
            
            // add content of last to columns
            String furi = featureNS + nextLine.get(0).toString();
            String fviewer = viewerURL + "?furi=" + furi;
            nextLine.put(fviewer);
            nextLine.put(furi);
            
            if (nextLine.length() == maxColLength.length) {
                for (int j = 0; j < nextLine.length(); j++) {
                    int l = nextLine.get(j).toString().length();
                    maxColLength[j] = l > maxColLength[j] ? l : maxColLength[j];
                }
            } 
            else return "";
        }
        int width = 0;
        for (int i = 0; i<maxColLength.length; i++) width += maxColLength[i];
        width += (maxColLength.length * 3) + 1; // gesamte tabellenbreite
        
        String sepV = "|"; // vertical separator
        String sepHChar = "-"; 
        String sepH = "";
        for(int i = 0; i < width; i++) sepH += sepHChar; // horizontal separator
        
        
        String table = sepH;
        table += "\n" + sepV;
        // table header durchlaufen, mit sepV trennen, jede zeile auffÃ¼llen bis zu maxColLength
        for (int i = 0; i < tableheaders.length(); i++) {
            table += getCellString(tableheaders.get(i).toString(), maxColLength[i]) + sepV;
        }
        table +=  "\n" + sepH;
        // jede zeile von table content durchlaufen, mit sepV trennen, jede zelle auffÃ¼llen bis zu maxColLength
        String lastline = "";
        for (int i = 0; i < tablecontent.length(); i++) {
            JSONArray nextLine = tablecontent.getJSONArray(i);
            String line = "\n" + sepV;
            for (int j = 0; j < nextLine.length(); j++) {
                line += getCellString(nextLine.get(j).toString(), maxColLength[j]) + sepV;
            }
            if(!line.equals(lastline)) {
                table += line;
                lastline = line;
                linecounter++;
            }
            
        }

        table +=  "\n" + sepH;
        return table;
    }
    
    private static String getCellString(String value, int totalLength) {
        while(totalLength > value.length()) {
            value += " ";
        }
        value = " " + value + " ";
        return value;
    }
    //</editor-fold>
}
