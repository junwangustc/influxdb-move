package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/influxdata/influxdb/client"
	v2 "github.com/influxdata/influxdb/client/v2"
)

func DBclient(host, port string) *client.Client {

	//connect to database
	u, err := url.Parse(fmt.Sprintf("http://%v:%v", host, port))
	if err != nil {
		fmt.Printf("Fail to parse host and port of database, error: %s\n", err.Error())
	}

	info := client.Config{
		URL: *u,
	}

	var con *client.Client
	con, err = client.NewClient(info)
	if err != nil {
		fmt.Printf("Fail to build newclient to database, error: %s\n", err.Error())
	}

	return con
}

func GetFieldKeys(ip, port, measurement, db string) map[string]string {
	//get measurements from database
	c, err := v2.NewHTTPClient(v2.HTTPConfig{
		Addr: "http://" + ip + ":" + port,
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}
	defer c.Close()

	fieldType := make(map[string]string)
	q := v2.NewQuery(fmt.Sprintf(`show field keys from "%s"`, measurement), db, "ns")
	if response, err := c.Query(q); err == nil && response.Error() == nil {
		fmt.Println("-=-=-=-=-=-=query-=-=-=-=-")
		res := response.Results[0].Series[0].Values

		for _, v := range res {
			if key, ok := v[0].(string); ok {
				if value, ok1 := v[1].(string); ok1 {
					fieldType[key] = value
				}

			}
		}
	} else {
		fmt.Println("error", err)
	}
	return fieldType
}
func Getmeasurements(c *client.Client, sdb, cmd string) []string {

	//get measurements from database
	q := client.Query{
		Command:  cmd,
		Database: sdb,
	}

	var measurements []string

	response, err := c.Query(q)
	if err != nil {
		fmt.Printf("Fail to get response from database, get measurements error: %s\n", err.Error())
	}

	res := response.Results

	if len(res[0].Series) == 0 {
		fmt.Printf("The response of database is null, get measurements error!\n")
	} else {

		values := res[0].Series[0].Values

		//show progress of getting measurements
		count := len(values)
		bar := pb.StartNew(count)

		for _, row := range values {
			measurement := fmt.Sprintf("%v", row[0])
			measurements = append(measurements, measurement)
			bar.Increment()
			time.Sleep(3 * time.Millisecond)
		}
		bar.FinishPrint("Get measurements has finished!\n")
	}
	return measurements

}

func ReadDB(c *client.Client, sdb, ddb, cmd string, fieldType map[string]string) client.BatchPoints {

	q := client.Query{
		Command:  cmd,
		Database: sdb,
	}

	//get type client.BatchPoints
	var batchpoints client.BatchPoints

	response, err := c.Query(q)
	if err != nil {
		fmt.Printf("Fail to get response from database, read database error: %s\n", err.Error())
	}

	res := response.Results
	if response.Err != nil {
		fmt.Println("读取DB失败", response.Err)
		return batchpoints
	}
	if len(res) == 0 {
		fmt.Printf("The response of database is null, read database error!\n")
	} else {

		res_length := len(res)

		fmt.Println("result总共返回数据组数", res_length)
		for k := 0; k < res_length; k++ {
			//show progress of reading series
			if res[k].Err != nil {
				fmt.Println("数据库读取失败", res[k].Err)
				continue
			}
			count := len(res[k].Series)
			bar := pb.StartNew(count)
			for _, ser := range res[k].Series {

				//get type client.Point
				var point client.Point

				point.Measurement = ser.Name
				point.Tags = ser.Tags

				for _, v := range ser.Values {
					point.Time, _ = time.Parse(time.RFC3339, v[0].(string))
					field := make(map[string]interface{})
					l := len(v)
					for i := 1; i < l; i++ {
						if v[i] != nil {
							//fmt.Println(ser.Columns[i], reflect.TypeOf(v[i]))
							if field_type, ok := fieldType[ser.Columns[i]]; ok {
								switch field_type {
								case "integer":
									var value int

									if newData, ok := v[i].(json.Number); ok {
										value, err = strconv.Atoi(string(newData))
										if err != nil {
											fmt.Println("convert type error")
										} else {
											field[ser.Columns[i]] = value
										}
									}

								case "float":
									var fValue float64
									if newData, ok := v[i].(json.Number); ok {
										fValue, err = strconv.ParseFloat(string(newData), 64)
										if err != nil {
											fmt.Println("convert type error")
										} else {
											field[ser.Columns[i]] = fValue
										}
									}

								case "string":
									field[ser.Columns[i]] = v[i]
								case "boolean":
									field[ser.Columns[i]] = v[i]
								default:
									fmt.Println("unkonw type")

								}

							} else {
								fmt.Println("error columns")
							}
						}
					}
					point.Fields = field
					point.Precision = "ns"
					batchpoints.Points = append(batchpoints.Points, point)
				}
				bar.Increment()
				time.Sleep(3 * time.Millisecond)
			}
			bar.FinishPrint("Read series has finished!\n")
		}
		batchpoints.Database = ddb
		batchpoints.RetentionPolicy = "autogen"
	}
	return batchpoints
}

func WriteDB(c *client.Client, b client.BatchPoints) {

	_, err := c.Write(b)
	if err != nil {
		fmt.Printf("Fail to write to database, error: %s\n", err.Error())
	}
}

func main() {

	//support to input source and destination hosts
	src := flag.String("s", "127.0.0.1", "input an ip of source DB, from which you want to output datas")
	dest := flag.String("d", "127.0.0.1", "input an ip of destination DB, from which you want to input datas")

	//support to input source and destination ports
	sport := flag.String("sport", "8086", "input a port of source DB,from which you want to output datas")
	dport := flag.String("dport", "8086", "input a port of destination DB,from which you want to input datas")

	//support to input source and destination database
	sdb := flag.String("sdb", "mydb", "input name of source DB, from which you want to output datas")
	ddb := flag.String("ddb", "yourdb", "input name of destination DB, from which you want to input datas")

	//support to input start time and end time during which you select series from database
	st := flag.String("stime", "1970-01-01 00:00:00", "input a start time ,from when you want to select datas")
	et := flag.String("etime", "2100-01-01 00:00:00", "input an end time, until when you want to select datas")

	flag.Parse()

	scon := DBclient(*src, *sport)
	dcon := DBclient(*dest, *dport)

	getmeasurements := "show measurements"
	measurements := Getmeasurements(scon, *sdb, getmeasurements)

	//show progress of writing to database
	count_outer := len(measurements)
	bar_outer := pb.StartNew(count_outer)

	for _, m := range measurements {

		fieldType := GetFieldKeys(*src, *sport, m, *sdb)
		fieldFill := ""
		fieldTmp := ""
		for k, v := range fieldType {
			fieldTmp += k
			fieldTmp += ","
			_ = v
		}
		fieldFill = fieldTmp[:len(fieldTmp)-1]
		fmt.Println("measurement is ", m, "Field FIll is ", fieldFill)
		template := "2006-01-02 15:04:05"

		since_time, err_sin := time.Parse(template, "1970-01-01 00:00:00")
		if err_sin != nil {
			fmt.Println("Fail to parse since_time")
		}

		stime, err_st := time.Parse(template, fmt.Sprintf("%v", *st))
		if err_st != nil {
			fmt.Println("Fail to parse stime")
		}

		etime, err_et := time.Parse(template, fmt.Sprintf("%v", *et))
		if err_et != nil {
			fmt.Println("Fail to parse etime")
		}

		s_epoch := stime.Sub(since_time)
		e_epoch := etime.Sub(since_time)

		h_length := int(e_epoch.Hours()-s_epoch.Hours()) + 1

		//The datas which can be inputed is less than a year
		if h_length > 8760 {
			h_length = 8760
		}

		fmt.Println("current measurement is ", m)
		//show progress of each measurement writing to database
		count := h_length
		bar := pb.StartNew(count)

		//write datas of every hour
		for i := 0; i < h_length; i++ {

			startsec := int(s_epoch.Seconds() + float64(i*3600))
			endsec := int(s_epoch.Seconds() + float64((i+1)*3600))

			getvalues := fmt.Sprintf("select %v from  \"%v\" where time  > %vs and time < %vs group by *", fieldFill, m, startsec, endsec)
			fmt.Println("SQL is [", getvalues, "]")
			batchpoints := ReadDB(scon, *sdb, *ddb, getvalues, fieldType)
			WriteDB(dcon, batchpoints)

			bar.Increment()
			time.Sleep(time.Millisecond)
		}
		bar.FinishPrint("Measurement:" + m + ",Write to Database has Finished")

		bar_outer.Increment()
		time.Sleep(time.Millisecond)
	}
	bar_outer.FinishPrint("Write to Database has Finished")
	fmt.Printf("Move datas from %s to %s has done!\n", *sdb, *ddb)
}
