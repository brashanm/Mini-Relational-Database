package main

import (
    "fmt"
    "log"
    "minidatabase/db"
)

func main() {
    // 1) Create and open the KV store.
    kv := &db.KV{Path: "test.db"}
    if err := kv.Open(); err != nil {
        log.Fatalf("Failed to open KV: %v", err)
    }
    defer kv.Close()

    database := &db.DB{
        Name: "MyTestDB",
        Kv: *kv,
    }


    rec := db.Record{}
    rec.AddStr("name", []byte("Alice"))
    rec.AddStr("location", []byte("Wonderland"))
    if added, err := database.Upsert("users", rec); err != nil {
        log.Fatalf("Upsert failed: %v", err)
    } else if added {
        fmt.Println("Inserted a new record.")
    } else {
        fmt.Println("Updated an existing record.")
    }

    queryRec := db.Record{}
    queryRec.AddStr("name", []byte("Alice"))
    found, err := database.Get("users", &queryRec)
    if err != nil {
        log.Fatalf("Get failed: %v", err)
    }
    if found {
        fmt.Printf("Retrieved record: %#v\n", queryRec)
    } else {
        fmt.Println("Record not found.")
    }
}