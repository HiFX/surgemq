package handlers

import (
	jwt "github.com/dgrijalva/jwt-go"
	"github.com/zenazn/goji/web"
	"github.com/HiFX/surgemq/models"
	"net/http"
	"time"
	"fmt"
	"encoding/json"
	"github.com/HiFX/surgemq/persistence"
)

var (
	tokenMissingError        authError = authError{Code : 1000, HttpStatus : 401, Message : "token is missing", }
	tokenInvalidError        authError = authError{Code : 1001, HttpStatus : 401, Message : "token is invalid", }
	clientInvalidError       authError = authError{Code : 1002, HttpStatus : 401, Message : "invalid client", }
)

type Authenticator struct {
	Base
	KeyFile     []byte
	ClientId    string
	Persist        *persistence.Redis
	ModeProd	bool
}

func (this Authenticator) Authenticate(c *web.C, h http.Handler) http.Handler {

	KeyReader := func(t *jwt.Token) (interface{}, error) {
		return this.KeyFile, nil
	}
	fn := func(w http.ResponseWriter, req *http.Request) {
		echoErr := func(customError authError) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(customError.HttpStatus)
			jMsg, _ := json.Marshal(customError)
			w.Write(jMsg)
		}
		//1. token existence check
		token, tokenError := jwt.ParseFromRequest(req, KeyReader)
		if tokenError != nil {
			//token not present in request
			echoErr(tokenMissingError)
			return
		}else {
			//2. token validity check
			if !token.Valid {
				//invalid token; return
				echoErr(tokenInvalidError)
				return
			}
			//token valid
			if this.ModeProd {
				//3. client id check
				if this.ClientId != token.Claims["aud"].(string) {
					echoErr(clientInvalidError)
					return
				}
			}
			//4. permissions check
			_, DToken := inspectTokenPermissions(token)
			c.Env["token"] = DToken
			fmt.Println("User Id : ", DToken.Sub)
		}
		h.ServeHTTP(w, req)
	}
	return http.HandlerFunc(fn)
}

func (this Authenticator) ChatToken(c web.C, w http.ResponseWriter, req *http.Request) {
	dToken, _ := c.Env["token"]
	token, _ := dToken.(models.Token)

	//todo : replace the user id generating code
	chatToken := fmt.Sprintf("%d", int(time.Now().UTC().Unix()))
	err := this.Persist.SetChatToken(chatToken, token)
	if err != nil {
		//todo : deal error
		return
	}
	this.Respond(w, 200, chatToken)
}

type authError struct {
	Code          int		`json:"code"`
	HttpStatus    int   	`json:"-"`
	Message       string	`json:"message"`
}

/**
inspectTokenPermissions checks whether the token in the request has
enough permissions as the application needs.
 */
func inspectTokenPermissions(authToken *jwt.Token) (bool, models.Token) {
	token := models.Token{}
	aud, audOk := authToken.Claims["aud"]
	sub, subOk := authToken.Claims["sub"]
	name, nameOk := authToken.Claims["name"]
	firstName, firstNameOk := authToken.Claims["firstName"]
	lastName, lastNameOk := authToken.Claims["lastName"]
	//	profileImage, profileImageOk := authToken.Claims["profileImage"]
	//	emailVerified, emailVerifiedOk := authToken.Claims["emailVerified"]
	//	email, emailOk := authToken.Claims["email"]

	if !(audOk && subOk && nameOk && firstNameOk && lastNameOk ) {
		return false, token
	}
	token.Aud = aud.(string)
	token.Sub = sub.(string)
	token.Name = name.(string)
	token.FirstName = firstName.(string)
	token.LastName = lastName.(string)
	//	if profileImageOk {
	//		token.ProfileImage = profileImage.(string)
	//	}
	//	token.EmailVerified = emailVerified.(string)
	//	token.Email = email.(string)

	return true, token
}

