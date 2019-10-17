/* Author: Cooper Thompson
   Project: Sample Golang - XML Data Script

   Description: The below applciation will download zip files from an HTTP directory, extract the XML files from each, and insert them into
   a Redis List. The application will verify uniquness using the filename checked against Redis keys. By using a "shared queue" concurrency model,
   and separating out the workload across multiple anonymous functions, the application is able to execute multiple componenets concurrently to speed
   up the data processing. Semaphores are used to limit the number of concurrent threads (go routines) within each step of the data processing workflow (file download, file extraction, database insertion).
   these parameters can be changed based on the system the application will be running on.
*/


package main

import (

	redis "gopkg.in/redis.v4"

	"fmt"
	"net/http"
	"log"
	"io/ioutil"
	"regexp"
	"strings"
	"archive/zip"
	"bytes"
	"sync"
	"time"
)

//The file type
type File struct {
  content string
  filename string
}

//URL to download data from
var URL = "http://feed.omgili.com/5Rh5AMTrc4Pv/mainstream/posts/"

//Redis Database Connection
var REDIS_CONNECTION = redis.NewClient(&redis.Options{
					        Addr:     "127.0.0.1:6379",
					        Password: "", // no password set
					        DB:       0,  // use default DB
			    		})

//List to dump data into 
var REDIS_LIST = "NEWS_XML"

//A buffer containing the XML news posts as instances of bytes.Buffer (shared queue concurrency model)
var ContentQueue = make(chan File, 100)
//A buffer containing the zip folders in reader type
var ZipQueue = make(chan *zip.Reader, 100)
//A buffer containing the links to the zip files
var LinkQueue = make(chan string, 100)

//The sync variable
var wg sync.WaitGroup



//Return the raw HTTP response from a URL in a string
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


//Parse all the links from a string of HTML using simple REGEX and string manipulation, and add all the links to the LinkQueue
func getLinks(_html_body string) {
	//Use regex to extract all of the links from the web directory
	re := regexp.MustCompile("\\s*(?i)href\\s*=\\s*(\"([^\"]*\")|'[^']*'|([^'\">\\s]+))")
	//Slice of all the matches in the HTML body matching the above regex
	raw_tags := re.FindAllString(_html_body, -1)
	//Iterate through all of the tags and remove the HREF tag, quote marks, and equal signs
	for i := 0; i < len(raw_tags); i++ {
		raw_tags[i] = strings.Replace(raw_tags[i], "href", "", -1)
		raw_tags[i] = strings.Replace(raw_tags[i], "\"", "", -1)
		raw_tags[i] = strings.Replace(raw_tags[i], "=", "", -1)
		//If the string is a zip file name, add to the links shared worker queue
		if strings.Contains(raw_tags[i], ".zip") {
			LinkQueue <- strings.TrimSpace(raw_tags[i])
		}
	}
	//Close the LinkQueue once all links have been added
	close(LinkQueue)
}

//Fetch zip file from URL and return archive/zip reader (default go pkg) to manipulate in memory
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

//Function to check unique data in redis
func isUnique(filename string) bool {
	client := REDIS_CONNECTION
	_, err := client.Get(filename).Result()
	if err == redis.Nil {
		log.Println(filename + " is unqiue")
		err = client.Set(filename, 1, 0).Err()
		if err!= nil {
			log.Fatal(err)
		}
		return true
	} else if err != nil {
		log.Println(err)
		return false
	} else {
		log.Println(filename + " is NOT unqiue")
		log.Println(err)
		return false
	}
}

//Function to insert the XML content into the Redis DB
func redisInsertList(xml File) {
	client := REDIS_CONNECTION
	if isUnique(xml.filename) {
		err := client.LPush(REDIS_LIST, xml.content).Err()
		log.Println(xml.filename +  " inserted into Redis DB")
	    if err != nil {
	        log.Println(err)
	    }
	}
}

//When a link has been added to the queue, download the zip file, pass the zip file contents into the 
func linkWorker(url string) {
    go func() {
    	//Sync the finish
		defer wg.Done()

		//Max number of HTTP requests that can happen at once
		maxConnections := 5
		sem := make(chan bool, maxConnections)
    	for link := range LinkQueue {
    		//Write to sempaphor
			sem <- true
			go func(link string) {
	    		log.Print("File Downloaded: " + link)
	    		//Get the contents of the zip file into memory
	    		r, err := getZipFile(url + "/" + link)
	    		//Error handling
				if err!= nil {
					log.Fatal(err)
				}
				//Put the zip folder into the Zip queue
	          	ZipQueue <- r
	        }(link)
        }
        //Attempt to Fill the semaphor back up to capacity to wait for all routines to finish
		for i := 0; i < cap(sem); i++ {
		    sem <- true
		}
        //Once the LinkQueue has been closed, close the ZipQueue
        close(ZipQueue)
    }()
}

//When a zip folder has been added to the queue, extract the contents, place contents in to content queue
func zipWorker() {
	go func() {
		//Sync the finish
		defer wg.Done()
		//Max number of zip folders that can be open at once
		maxFolders := 30
		sem := make(chan bool, maxFolders)
		//Process zip folders as they enter the queue
		for _zip := range ZipQueue {
			//Write to sempaphor
			sem <- true
			//Function to open each file in the zip
			go func(_zip *zip.Reader) {
				//Free up a slot in the sempaphor after function is complete
			    defer func() { <-sem }()
				//Iterate over all the files in the zip folder
				for _, f := range _zip.File {
					//Print the file name for debugging 
		    		log.Print("File Extracted: " + f.Name)
		    		//Open the file in memory
		    		rc, err := f.Open()
			        if err != nil {
			        	log.Fatal(err)
			        }
			        //Add the contents of the file to the content queue
					buf := new(bytes.Buffer)
					buf.ReadFrom(rc)
					ContentQueue <- File{filename: f.Name, content: buf.String()}
		    	}
			}(_zip)
		}
		//Attempt to Fill the semaphor back up to capacity to wait for all routines to finish
		for i := 0; i < cap(sem); i++ {
		    sem <- true
		}
		//Once the ZipQueue has been closed, close the contentQueue
		close(ContentQueue)
	}()
}

//When content is added to the queue, insert into redis
func redisWorker() {
	go func() {
		//Sync the finish
		defer wg.Done()
		
		//Process content as it enters the queue
		for data := range ContentQueue {
			redisInsertList(data)
			time.Sleep(100 * time.Millisecond)
		}
	}()
}


func main() {
	//Set the URL
	url := URL
	//Wait for the 3 workers to finish
	wg.Add(3)
	//Fire off the linkworker
	linkWorker(url)
	log.Print("Link Worker started...")
	//Fire off the zipWorker
	zipWorker()
	log.Print("Zip Worker started...")
	//Fire off the redisWorker
	redisWorker()
	log.Print("Redis Worker started...")
	//Get links from the HTTP directory, and load them into the link queue
	getLinks(get(url))
	//Wwait for all concurrent workers to finish
	wg.Wait()
}




