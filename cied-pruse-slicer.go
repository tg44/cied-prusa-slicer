package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/streadway/amqp"
)

func main() {

	AMQP_URL := readEnvOrFallback("AMQP_URL", "amqp://guest:guest@localhost:5672/")
	AMQP_RECQ := readEnvOrFallback("AMQP_RECQ", "prusa-jobs")
	AMQP_JOBDONEQ := readEnvOrFallback("AMQP_JOBDONEQ", "done-jobs")
	S3_REGION := readEnvOrFallback("S3_REGION", "us-east-1")
	S3_BUCKET := readEnvOrFallback("S3_BUCKET", "newbucket")
	S3_ENDPOINT := readEnvOrFallback("S3_ENDPOINT", "http://localhost:9000")
	S3_ACCESSKEYID := readEnvOrFallback("S3_ACCESSKEYID", "TESTKEY")
	S3_SECRETACCESSKEY := readEnvOrFallback("S3_SECRETACCESSKEY", "TESTSECRET")
	S3_DISABLESSL, err := strconv.ParseBool(readEnvOrFallback("S3_DISABLESSL", "true"))

	failOnError(err, "Failed to parse S3_DISABLESSL")

	_, err = exec.LookPath("prusa-slicer")
	failOnError(err, "PruseSlicer not installed!")

	conn, err := amqp.Dial(AMQP_URL)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	receiveQueue, err := ch.QueueDeclare(
		AMQP_RECQ, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//make it fair
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	senderQueue, err := ch.QueueDeclare(
		AMQP_JOBDONEQ, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(S3_ACCESSKEYID, S3_SECRETACCESSKEY, ""),
		Endpoint:         aws.String(S3_ENDPOINT),
		Region:           aws.String(S3_REGION),
		DisableSSL:       aws.Bool(S3_DISABLESSL),
		S3ForcePathStyle: aws.Bool(true),
	}
	s, err := session.NewSession(s3Config)
	failOnError(err, "Failed to start S3 session")

	forever := make(chan bool)

	receive(*ch, receiveQueue, senderQueue, s, S3_BUCKET)
	log.Printf("Server started!")

	<-forever
}

func receive(ch amqp.Channel, queue amqp.Queue, doneQueue amqp.Queue, s *session.Session, bucket string) {
	msgs, err := ch.Consume(
		queue.Name, // queue
		"",     // consumer
		false,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			jobid, command, stdout, stderr, outpath, runtime, err := processJob(d.Body, s, bucket)
			if jobid == "" {
				log.Printf("There was a json parse error! %s", d.Body)
			} else {
				doneMsg := FinishedJobMessage{jobid, outpath, err, runtime, stdout, stderr, command}
				send(ch, doneQueue, doneMsg)
				if err != nil {
					log.Printf("%s failed in %d ms with %s", jobid, runtime, err.Error())
				} else {
					log.Printf("%s finished in %d ms", jobid, runtime)
				}
			}
			d.Ack(false)
		}
	}()
}

func send(ch amqp.Channel, queue amqp.Queue, msg FinishedJobMessage) {
	body, err := json.Marshal(msg)
	if err!=nil {
		log.Printf("We couldn't publish! %s", msg.JobID)
		return
	}
	err = ch.Publish(
		"",     // exchange
		queue.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(body),
		})
	if err!=nil {
		log.Printf("We couldn't publish! %s", msg.JobID)
	}
}

//jobid, command, stdout, stderr, fileurl, time, error
func processJob(js []byte, s *session.Session, s3Bucket string) (string, string, string, string, string, int64, error) {
	start := time.Now()

	var parsedMsg JobMessage
	err := json.Unmarshal(js, &parsedMsg)
	if err != nil {
		return "", "", "", "", "", time.Since(start).Milliseconds(), err
	}
	log.Printf("%s starting job", parsedMsg.JobID)
	dir, err := ioutil.TempDir("", "job")
	if err != nil {
		return parsedMsg.JobID, "", "", "", "", time.Since(start).Milliseconds(), err
	}
	defer os.RemoveAll(dir)
	err = downloadFiles(dir, parsedMsg.File, parsedMsg.ConfigFile)
	if err != nil {
		return parsedMsg.JobID, "", "", "", "", time.Since(start).Milliseconds(), err
	}
	log.Printf("%s download completed", parsedMsg.JobID)
	command, stdOut, stdErr, err := runSlicer(parsedMsg, dir)
	if err != nil {
		return parsedMsg.JobID, command, stdOut, stdErr, "", time.Since(start).Milliseconds(), err
	}
	log.Printf("%s render completed", parsedMsg.JobID)
	url, err := uploadFileToS3(s, s3Bucket, filepath.Join(dir, "output.gcode"), filepath.Join(parsedMsg.JobID, "output.gcode"))
	if err != nil {
		return parsedMsg.JobID, command, stdOut, stdErr, "", time.Since(start).Milliseconds(), err
	}

	return parsedMsg.JobID, command, stdOut, stdErr, url, time.Since(start).Milliseconds(), nil
}

func downloadFiles(dir string, file string, configFile string) error {
	err := downloadOneFile(dir, file)
	if err != nil {
		return err
	}
	err = downloadOneFile(dir, configFile)
	if err != nil {
		return err
	}
	return nil
}

func downloadOneFile(dir string, url string) error {
	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(filepath.Join(dir, path.Base(resp.Request.URL.String())))
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	return err
}

func runSlicer(jm JobMessage, dir string) (string, string, string, error) {
	cmd := exec.Command("prusa-slicer")
	cmd.Dir = dir
	cmd.Args = append(cmd.Args, "-g", path.Base(jm.File))
	cmd.Args = append(cmd.Args, "--load", path.Base(jm.ConfigFile))
	cmd.Args = append(cmd.Args, "--output", "output.gcode")

	var params = jm.normalizeParams()
	for key, value := range params {
		cmd.Args = append(cmd.Args, fmt.Sprintf("--%s", key), fmt.Sprintf("%s", value))
	}
	var out bytes.Buffer
	cmd.Stdout = &out

	var out2 bytes.Buffer
	cmd.Stderr = &out2

	err := cmd.Run()

	if err != nil {
		return strings.Join(cmd.Args," "), out.String(), out2.String(), err
	}
	return strings.Join(cmd.Args," "), out.String(), out2.String(), nil
}

func uploadFileToS3(s *session.Session, s3Bucket string, fileDir string, outFileName string) (string, error) {
	// Open the file for use
	file, err := os.Open(fileDir)
	if err != nil {
		return "", err
	}
	defer file.Close()

	uploader := s3manager.NewUploader(s)
	uploadRes, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(outFileName),
		Body:   file,
	})
	if uploadRes != nil {
		return uploadRes.Location, err
	}
	return "", err
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func readEnvOrFallback(env string, defVal string) string {
	out, succ := os.LookupEnv(env)
	if succ {
		return out
	}
	return defVal
}

type FinishedJobMessage struct {
	JobID  string   `json:"jobId"`
	File string     `json:"file"`
	Error error `json:"error"`
	Runtime int64 `json:"runTime"`
	StdOut string `json:"stdOut"`
	StdErr string `json:"stdErr"`
	CommandInfo string `json:"commandInfo"`
}

type JobMessage struct {
	JobID  string   `json:"jobId"`
	ConfigFile string     `json:"profileFile"`
	File  string `json:"file"`
	ParamsRaw map[string]interface{} `json:"params"`
}

func (jm JobMessage) normalizeParams() map[string]string {
	//todo: we should normalize the params between cura and slicer
	data := make(map[string]string)
	for key, value := range jm.ParamsRaw {
		switch v := value.(type) {
		case int:
			data[key] = strconv.Itoa(v)
		case float64:
			data[key] = strconv.FormatFloat(v, 'f', 4, 64)
		case string:
			data[key] = `"` + v + `"`
		case bool:
			data[key] = strconv.FormatBool(v)
		default:
		}
	}
	return data
}
