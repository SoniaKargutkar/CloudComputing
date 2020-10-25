package MapReduce.Master;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLEncoder;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;


public class Gcloudstopinstance
{

	public static void stopInstances(List<String> instancesList, String projectID, String zone)
            throws IOException, GeneralSecurityException {
        //HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        //JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

        GoogleCredential credential = GoogleCredential.getApplicationDefault();
        if (credential.createScopedRequired()) {
            credential =
                    credential.createScoped(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
        }
        credential.refreshToken();
        String accessToken = credential.getAccessToken();



        System.out.println("Access Token: " + accessToken);

        for (int i = 0; i < instancesList.size(); i++) {

            String instance = instancesList.get(i);

            URL url;
            try {
                url = new URL("https://compute.googleapis.com/compute/v1/projects/" + projectID + "/zones/" + zone
                        + "/instances/" + instance + "/stop");

                HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
                conn.setRequestMethod("POST");

                conn.setRequestProperty("Authorization", "Bearer " + accessToken);

                conn.setRequestProperty("Content-Type", "application/json; utf-8");
                conn.setRequestProperty("Accept", "application/json");
                conn.setDoOutput(true);

                Map<String, String> params = new HashMap<>();
                params.put("project", projectID);
                params.put("zone", zone);
                params.put("resourceId", instance);

                StringBuilder requestData = new StringBuilder();

                for (Map.Entry<String, String> param : params.entrySet()) {
                    if (requestData.length() != 0) {
                        requestData.append('&');
                    }
                    // Encode the parameter based on the parameter map we've defined
                    // and append the values from the map to form a single parameter
                    requestData.append(URLEncoder.encode(param.getKey(), "UTF-8"));
                    requestData.append('=');
                    requestData.append(URLEncoder.encode(String.valueOf(param.getValue()), "UTF-8"));
                }

                // Convert the requestData into bytes
                byte[] requestDataByes = requestData.toString().getBytes("UTF-8");
                conn.setDoOutput(true);

                // Get the output stream of the connection instance
                // and add the parameter to the request
                try (DataOutputStream writer = new DataOutputStream(conn.getOutputStream())) {
                    writer.write(requestDataByes);

                    // Always flush and close
                    writer.flush();
                    writer.close();

                    StringBuilder content;

                    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                        String line;
                        content = new StringBuilder();
                        while ((line = in.readLine()) != null) {
                            content.append(line);
                            content.append(System.lineSeparator());
                        }
                    }
                    System.out.println(content.toString());
                } finally {
                    conn.disconnect();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}
