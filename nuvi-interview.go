package main

import (

	redis "github.com/xuyu/goredis"

	"fmt"
	"net/http"
	"log"
	"io/ioutil"
	"regexp"
	"strings"
	"archive/zip"
	 "bytes"
)

func get(_addr string) string {
	res, err := http.Get(_addr)
	if err != nil {
		log.Fatal(err)
	}
	response, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	resp_body := string(response)
	return resp_body
}

func getZipFile(url string) (*zip.Reader, error) {
	res, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf(
			"Fetching zip URL %s failed with error %s.", url, err)
	}
	defer res.Body.Close()
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read response body with error %s", err)
	}
	r := bytes.NewReader(b)
	return zip.NewReader(r, int64(r.Len()))
}

func parseLinks(_html_body string) []string {
	//Use regex to extract all of the links from the web directory
	re := regexp.MustCompile("\\s*(?i)href\\s*=\\s*(\"([^\"]*\")|'[^']*'|([^'\">\\s]+))")
	//Slice of all the matches in the HTML body matching the above regex
	raw_tags := re.FindAllString(_html_body, 6)
	//Slice of all the parsed and filtered links
	var links []string
	//Iterate through all of the tags and remove the HREF tag, quote marks, and equal signs
	for i := 0; i < len(raw_tags); i++ {
		raw_tags[i] = strings.Replace(raw_tags[i], "href", "", -1)
		raw_tags[i] = strings.Replace(raw_tags[i], "\"", "", -1)
		raw_tags[i] = strings.Replace(raw_tags[i], "=", "", -1)
		if strings.Contains(raw_tags[i], ".zip") {
			links = append(links, strings.TrimSpace(raw_tags[i]))
		}
	}
	fmt.Println(links)
	return links
}

func redisInsert(content string) {
	client, _ := redis.Dial(&redis.DialConfig{Address: "127.0.0.1:6379"})
	_, err := client.ExecuteCommand("LPUSH", "NEWS_XML", content)
	if err!= nil {
		log.Fatal(err)
	}


}

func main() {
	url := "http://feed.omgili.com/5Rh5AMTrc4Pv/mainstream/posts/"
	links := parseLinks(get(url))
	for _, link := range links {
		r, err := getZipFile(url + "/" + link)
		fmt.Println(url + "/" + link)
		if err!= nil {
			log.Fatal(err)
		}
		for _, f := range r.File {
    		log.Print(f.Name)
    		rc, err := f.Open()
	        if err != nil {
	        	log.Fatal(err)
	        }
			buf := new(bytes.Buffer)
			println(buf.String())
			buf.ReadFrom(rc)
			redisInsert(buf.String())
    	}
	}



}




