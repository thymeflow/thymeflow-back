package pkb.sync.utils

import java.awt.Desktop
import java.net.URI
import java.util.Scanner

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.HttpResponse
import org.apache.http.auth.AuthenticationException
import org.apache.http.client.HttpClient
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair

import scala.collection.JavaConverters._

object OAuth2 {
  val Google = new OAuth2(
    "https://accounts.google.com/o/oauth2/v2/auth",
    "https://www.googleapis.com/oauth2/v4/token",
    "503500000487-nutchjm39fo3p5l171cqo1k5lsprjpau.apps.googleusercontent.com",
    "39tNR9btCqLNKIriJFY28Yop" //TODO: remove before making the repository public
  )

  @JsonIgnoreProperties(ignoreUnknown = true)
  private class Token(
                       @JsonProperty("access_token") accessToken: String,
                       @JsonProperty("expires_in") expiresIn: Long,
                       @JsonProperty("token_type") tokenType: String
                     ) {

    def getAccessToken: String = {
      accessToken
    }

    def getExpiresIn: Long = {
      expiresIn
    }

    def getTokenType: String = {
      tokenType
    }
  }

}

class OAuth2(authorizeUri: String, tokenUri: String, clientId: String, clientSecret: String) {

  def getAccessToken(scopes: Iterable[String]): String = {
    val googleHttpClient: HttpClient = HttpClients.createDefault
    try {
      //launch authorization code request
      val uri = new URI("https://accounts.google.com/o/oauth2/v2/auth?" +
        "scope=" + scopes.mkString("%20") + "&" +
        "redirect_uri=urn:ietf:wg:oauth:2.0:oob&" +
        "response_type=code&" +
        "client_id=" + clientId)
      Desktop.getDesktop.browse(uri)

      //retrieve the authorization code
      val scanner = new Scanner(System.in)
      System.out.println("Input your authorization code:")
      val code = scanner.next

      //get access token
      val postMethod = new HttpPost("https://www.googleapis.com/oauth2/v4/token")
      val parameters = Array(
        new BasicNameValuePair("code", code),
        new BasicNameValuePair("client_id", clientId),
        new BasicNameValuePair("client_secret", clientSecret),
        new BasicNameValuePair("redirect_uri", "urn:ietf:wg:oauth:2.0:oob"),
        new BasicNameValuePair("grant_type", "authorization_code")
      )
      postMethod.setEntity(new UrlEncodedFormEntity(parameters.toList.asJava))
      val response: HttpResponse = googleHttpClient.execute(postMethod)
      (new ObjectMapper).readValue(response.getEntity.getContent, classOf[OAuth2.Token]).getAccessToken
    }
    catch {
      case e: Exception => throw new AuthenticationException("Google authentification failed: " + e.getMessage, e)
    }
  }
}

