package models

type Token struct {
	Iss           string
	Aud           string
	Sub           string
	Name          string
	FirstName     string
	LastName      string
	ProfileImage  string
	EmailVerified string
	Email         string
	Nonce         string
	At_hash       string
	Iat           int
	Exp           int
}
