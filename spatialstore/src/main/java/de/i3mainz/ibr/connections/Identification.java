package de.i3mainz.ibr.connections;

import de.i3mainz.ibr.database.DBInterface;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.apache.oltu.oauth2.client.OAuthClient;
import org.apache.oltu.oauth2.client.URLConnectionClient;
import org.apache.oltu.oauth2.client.request.OAuthBearerClientRequest;
import org.apache.oltu.oauth2.client.request.OAuthClientRequest;
import org.apache.oltu.oauth2.client.response.OAuthAuthzResponse;
import org.apache.oltu.oauth2.client.response.OAuthJSONAccessTokenResponse;
import org.apache.oltu.oauth2.client.response.OAuthResourceResponse;
import org.apache.oltu.oauth2.common.OAuth;
import org.apache.oltu.oauth2.common.OAuthProviderType;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import org.apache.oltu.oauth2.common.message.types.GrantType;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Identification {

    private static String[] admins = {"martin.unold@gmail.com", "thiery.florian@googlemail.com", "kaischib@gmail.com", "alexmue90@gmail.com"};
    private static HashMap<String, Identification> users = new HashMap<String, Identification>();
    private static final Properties config = new Properties();
    private static final long maxLoginTime = 14400000; //4 Stunden: 14400000

    private String userID;
    private Date loginDate;
    private boolean isAdmin;
    private int visibilityState; // 0 = NOTHING, 2 = IN PROGRESS, 4 = READY

    static {
        try {
            //config.load(new FileReader("config.properties"));
            config.load(Identification.class.getClassLoader().getResourceAsStream("config.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static final String host = config.getProperty("gv_host");
    private static final String viewerAdress = config.getProperty("gv_viewer");
    //private static final String restAdress = config.getProperty("gv_rest");
    private static final String openIDAdress = config.getProperty("gv_openid");

    private Identification() {
        userID = null;
        loginDate = new Date();
        isAdmin = false;
        visibilityState = 0;
    }

    private void setId(String id) {
        userID = id;
        isAdmin = false;
        for (String admin : admins) {
            if (admin.equals(userID)) {
                isAdmin = true;
            }
        }
    }

    public String getID(int fid) throws ClientException, Exception {
        if (fid != 0) {
            if (isAdmin) {
                return userID;
            }
            String creator;
            try (Database db = new Database()) {
                creator = db.getCreator(fid);
                db.close();
            } catch (Exception e) {
                throw e;
            }
            if (creator == null || creator.equals("null")) {
                return userID;
            }
            if (creator.equals(userID)) {
                return userID;
            }
            throw new ClientException("feature " + fid + " is created by " + creator, 401);
        }
        return userID;
    }

    public void setState(int state) {
        visibilityState = state;
    }

    public int getState() {
        return visibilityState;
    }

    public static Response LoginPage(HttpServletRequest req) {
        String html = "";

        html += "<html>";
        html += "<head>";
        html += "<title>JQuery Simple OpenID Selector Demo</title>";
        html += "<style type=\"text/css\">";
        html += "body {";
        html += "font-family: \"Helvetica Neue\", Helvetica, Arial, sans-serif;";
        html += "color:black;";
        html += "}";
        html += "</style>";
        html += "</head>";
        //html += "<body bgcolor=\"#bc4a40\">";
        html += "<body>";
        html += "<br /><br /><center>";
        html += "<img src=\"" + viewerAdress + "/img/genericviewer.png\" alt=\"\" width=\"50%\"/><br /><br /><br />";
        html += "<fieldset style=\"width:80%;background-color:white;\">";
        html += "<legend style=\"color:black;\"><b>OpenID Sign-in or Create New Account - Please click your account provider</b></legend>";
        html += "<table width=\"95%\" border=\"0\" align=\"center\" cellpadding=\"0\" cellspacing=\"0\">";
        html += "<tr>";
        html += "<td width=\"50%\" height=\"200\" align=\"center\" valign=\"middle\"><center><a href=\"" + openIDAdress + "/login/Google\"><img src=\"" + viewerAdress + "/img/google.jpg\" alt=\"Google\" /></a></center></td>";
        //html += "<td width=\"50%\" height=\"200\" align=\"center\" valign=\"middle\"><center><a href=\"" + openIDAdress + "/login/Yahoo\"><img src=\"" + viewerAdress + "/img/yahoo.jpg\" alt=\"Yahoo\" /></a></center></td>";
        html += "</tr>";
        html += "</table>";
        html += "</fieldset>";
        html += "<h2>Was ist OpenID?</h2>";
        html += "<p>OpenID ist ein Service, der es Ihnen erlaubt mit einem Login sich in vielen verschiedenen Websiten einzuloggen.";
        html += "<br>Mehr Informationen finden Sie <a href=\"http://openid.net/what/\" target=\"_blank\">hier</a>."
                + "<br>Wie sie einen OpenID Account erwerben kÃƒÆ’Ã‚Â¶nnen finden Sie <a href=\"http://openid.net/get/\" target=\"_blank\">hier</a>.</p>";
        html += "<h2>Welche Informationen werden genutzt?</h2>";
        html += "<p>OpenID ermÃƒÂ¶glicht den Zugriff auf alle von Ihnen mitgeteilten Metadaten, z.B. Name, Email und Geburtsdatum.";
        html += "<br>IBR - Generic Viewer - nutzt zur Identifizierung lediglich Ihre hinterlegte Emailadresse.";
        html += "</center>";
        html += "</body>";
        html += "</html>";

        return Response.ok(html).header("Access-Control-Allow-Origin", "*").build();
    }

    public static Identification getUser(HttpServletRequest req) {
        String id = req.getSession().getId();
        if (!users.containsKey(id)) {
            users.put(id, new Identification());//anonymous user?

        }

        return users.get(id);
    }

    static String CLIENT_ID = "907218053948-os316nu48j6ntntuuuiggge88e2gkcb5.apps.googleusercontent.com";
    static String CLIENT_SECRET = "6K1M36a_34wXzEae8MjMdljG";
    static String APPLICATION_NAME = "genericviewer";
    static String REDIRECT_URI = config.getProperty("gv_openid") + "/identify";
    
    /**
     * Login for provider.
     * Quickstart Guide for oltu lib: https://cwiki.apache.org/confluence/display/OLTU/OAuth+2.0+Client+Quickstart
     * 
     * @param req
     * @param provider
     * @return 
     */
    public static Response providerLogin(HttpServletRequest req, String provider) {
        try {
            System.out.println(REDIRECT_URI);
                    
            OAuthClientRequest request = OAuthClientRequest
                    .authorizationProvider(OAuthProviderType.GOOGLE)
                    .setClientId(CLIENT_ID)
                    .setResponseType("code")
                    .setScope("openid email")
                    .setRedirectURI(REDIRECT_URI)
                    .buildQueryMessage();
            

            return Response.seeOther(URI.create(request.getLocationUri())).build();

        } catch (OAuthSystemException ex) {
            Logger.getLogger(Identification.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

    /**
     * Authenticate at authentication provider.
     * Quickstart Guide for oltu lib: https://cwiki.apache.org/confluence/display/OLTU/OAuth+2.0+Client+Quickstart
     * 
     * @param req
     * @return 
     */
    public static Response identify(HttpServletRequest req) {
        String accessToken;
        OAuthClient oAuthClient = new OAuthClient(new URLConnectionClient());
        try {

            OAuthAuthzResponse oar = OAuthAuthzResponse.oauthCodeAuthzResponse(req);
            String code = oar.getCode();
            
            System.out.println("URI: " + REDIRECT_URI);
            OAuthClientRequest request = OAuthClientRequest
                    .tokenProvider(OAuthProviderType.GOOGLE)
                    .setGrantType(GrantType.AUTHORIZATION_CODE)
                    .setClientId(CLIENT_ID)
                    .setClientSecret(CLIENT_SECRET)
                    .setRedirectURI(REDIRECT_URI)
                    .setCode(code)
                    .buildBodyMessage();

            OAuthJSONAccessTokenResponse accessTokenResponse = oAuthClient.accessToken(request, "POST");
            accessToken = accessTokenResponse.getAccessToken();
            String expiresIn = accessTokenResponse.getExpiresIn().toString();

            OAuthClientRequest bearerClientRequest = new OAuthBearerClientRequest("https://www.googleapis.com/plus/v1/people/me/openIdConnect").setAccessToken(accessToken).buildQueryMessage();
            OAuthResourceResponse resourceResponse = oAuthClient.resource(bearerClientRequest, OAuth.HttpMethod.GET, OAuthResourceResponse.class);

            if (resourceResponse.getResponseCode() == 200) {
                JSONObject json = new JSONObject(resourceResponse.getBody());
                String email = json.getString("email");
                System.out.println(email);
                Identification user = getUser(req);
                user.setId(email);

            } else {
                System.out.println("login failed");
            }

        } catch (OAuthProblemException | OAuthSystemException | JSONException ex) {
            Logger.getLogger(Identification.class.getName()).log(Level.SEVERE, null, ex);
        }
        return Config.getResult();
    }

    public static Response getUserData(HttpServletRequest req) {
        Identification user = getUser(req);
        String message = Config.xml
                + "<log>"
                + "<user>" + user.userID + "</user>"
                + "<status>" + user.visibilityState + "</status>"
                + "<session>" + req.getSession().getId() + "</session>"
                + "</log>";
        return Config.getResult(message);
    }

    public static void refresh() {
        try {
            Date now = new Date();
            for (String id : users.keySet()) {
                long timeDiff = now.getTime() - users.get(id).loginDate.getTime();
                if (timeDiff > maxLoginTime) {
                    users.remove(id);
                }
            }
        } catch (Exception e) {
            Config.warn("Error during Identification.refresh: " + e);
        }
    }

    public static Response logout(HttpServletRequest req) {
        Identification user = getUser(req);
        user.setId(null);
        return getUserData(req);
    }

    public static String getUser(HttpServletRequest req, int fid) throws ClientException, Exception {
        Identification user = getUser(req);
        if (fid != 0) {
            if (user.isAdmin) {
                return user.userID;
            }
            String creator;
            try (Database db = new Database()) {
                creator = db.getCreator(fid);
                db.close();
            } catch (Exception e) {
                throw e;
            }
            if (creator == null || creator.equals("null")) {
                return user.userID;
            }
            if (creator.equals(user.userID)) {
                return user.userID;
            }
            throw new ClientException("feature " + fid + " is created by " + creator, 401);
        }
        return user.userID;
    }

    public static boolean isAdmin(HttpServletRequest req) {
        Identification user = getUser(req);
        return user.isAdmin;
    }

}

class Database extends DBInterface {

    public Database() throws SQLException, ClassNotFoundException {
        super();
    }

    public String getCreator(int fid) throws SQLException, ClientException {
        String sql = "SELECT creator FROM edit WHERE IDREF_feature = ? AND date = (SELECT max(date) FROM edit WHERE IDREF_feature = ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setInt(1, fid);
        statement.setInt(2, fid);
        ResultSet result = statement.executeQuery();
        if (!result.next()) {
            throw new ClientException("feature " + fid + " does not exist", 404);
        }
        return result.getString("creator");
    }

}
