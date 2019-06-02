package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/gocql/gocql"
)

type dp struct {
	ID    gocql.UUID
	TSss  int
	Dataa float32
}

func main() {
	var gocqlUUID gocql.UUID
	var data1, data2, data3, data4, data5, data6 dp
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	// connect to the cluster
	cluster := gocql.NewCluster("192.168.2.101", "192.168.2.25", "192.168.2.35") //replace PublicIP with the IP addresses used by your cluster.
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.ConnectTimeout = time.Second * 10
	//cluster.Authenticator = gocql.PasswordAuthenticator{Username: "Username", Password: "Password"} //replace the username and password fields with their real settings.
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	fmt.Println("Cassandra Init Done")
	defer session.Close()

	// create keyspaces
	err = session.Query("CREATE KEYSPACE IF NOT EXISTS firsttest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor' : 3};").Exec()
	if err != nil {
		log.Println(err)
		return
	}

	// create table
	err = session.Query("CREATE TABLE IF NOT EXISTS firsttest.tsdata (deviceid uuid, ts timeuuid,data float ,primary key(deviceid, ts)) WITH CLUSTERING ORDER BY (ts DESC) AND compaction = {'class': 'TimeWindowCompactionStrategy','compaction_window_size': 1,'compaction_window_unit': 'DAYS'};").Exec()
	if err != nil {
		log.Println(err)
		return
	}

	gocqlUUID, _ = gocql.RandomUUID()
	data1.ID = gocqlUUID
	gocqlUUID, _ = gocql.RandomUUID()
	data2.ID = gocqlUUID
	gocqlUUID, _ = gocql.RandomUUID()
	data3.ID = gocqlUUID
	gocqlUUID, _ = gocql.RandomUUID()
	data4.ID = gocqlUUID
	gocqlUUID, _ = gocql.RandomUUID()
	data5.ID = gocqlUUID
	gocqlUUID, _ = gocql.RandomUUID()
	data6.ID = gocqlUUID

	for i := 0; i < 10000000; {

		data1.Dataa = r1.Float32() * 60
		data2.Dataa = r1.Float32() * 60
		data3.Dataa = r1.Float32() * 60
		data4.Dataa = r1.Float32() * 60
		data5.Dataa = r1.Float32() * 120
		data6.Dataa = r1.Float32() * 500

		// write data to Cassandra
		err = session.Query("INSERT INTO firsttest.tsdata (deviceid, ts, data) VALUES (?, ?, ?);", data1.ID, gocql.TimeUUID(), data1.Dataa).Exec()
		err = session.Query("INSERT INTO firsttest.tsdata (deviceid, ts, data) VALUES (?, ?, ?);", data2.ID, gocql.TimeUUID(), data2.Dataa).Exec()
		err = session.Query("INSERT INTO firsttest.tsdata (deviceid, ts, data) VALUES (?, ?, ?);", data3.ID, gocql.TimeUUID(), data3.Dataa).Exec()
		err = session.Query("INSERT INTO firsttest.tsdata (deviceid, ts, data) VALUES (?, ?, ?);", data4.ID, gocql.TimeUUID(), data4.Dataa).Exec()
		err = session.Query("INSERT INTO firsttest.tsdata (deviceid, ts, data) VALUES (?, ?, ?);", data5.ID, gocql.TimeUUID(), data5.Dataa).Exec()
		err = session.Query("INSERT INTO firsttest.tsdata (deviceid, ts, data) VALUES (?, ?, ?);", data6.ID, gocql.TimeUUID(), data6.Dataa).Exec()

		if err != nil {
			fmt.Println("errors", err)
		} else {
			fmt.Println("Cassandra Insert " + strconv.Itoa(i) + " Done")
		}

		i = i + 1 //add 60 seconds
		time.Sleep(5 * time.Millisecond)
	}
	/*
		gocqlUUID, _ = gocql.RandomUUID()
		data.ID = gocqlUUID
		data.TS = r1.Int31()
		data.Data = r1.Float32()

		// write data to Cassandra
		err = session.Query(`INSERT INTO firsttest.tsdata (id, ts, data) VALUES (?, ?, ?)`, data.ID, data.TS, data.Data).Exec()

		if err != nil {
			fmt.Println("errors", err)
		} else {
			fmt.Println("Cassandra Insert 2 Done")
		}

		gocqlUUID, _ = gocql.RandomUUID()
		data.ID = gocqlUUID
		data.TS = r1.Int31()
		data.Data = r1.Float32()

		// write data to Cassandra
		err = session.Query(`INSERT INTO firsttest.tsdata (id, ts, data) VALUES (?, ?, ?)`, data.ID, data.TS, data.Data).Exec()

		if err != nil {
			fmt.Println("errors", err)
		} else {
			fmt.Println("Cassandra Insert 3 Done")
		}

		/*
			CREATE TABLE tsdata (id UUID, ts int, data float,PRIMARY KEY (id, ts));
	*/
}
