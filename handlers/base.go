package handlers

import (
	"net/http"
	"encoding/json"
)

type Base struct {

}

func (this *Base) Respond(w http.ResponseWriter, httpStatus int, data interface {}){
	jRes, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_, err := w.Write(jRes)
	if err != nil {
		//todo : deal error
	}
}
