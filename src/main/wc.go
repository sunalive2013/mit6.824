package main

import (
	"fmt"
	"mapreduce"
	"os"
	"strings"
	"unicode"
	"strconv"
	"log"
)

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func mapF(filename string, contents string) []mapreduce.KeyValue {
	// Your code here (Part II).
	//根据输入的文件名和文件目录，忽略输入文件名，直挂差目录参数，返回一个kv切片
	//先对于文件内容value进行分割，用strings.FieldsFunc函数来分割成单词。
	// 然后对于每个单词，将[word,”1”]加入到中间结果中。

	// FieldsFunc 以一个或多个满足 f(rune) 的字符为分隔符，
	// 将 s 切分成多个子串，结果中不包含分隔符本身。
	// 如果 s 中没有满足 f(rune) 的字符，则返回一个空列表。
	//func FieldsFunc(s string, f func(rune) bool) []string
	var ans []mapreduce.KeyValue
	wordvalues := strings.FieldsFunc(contents, func(r rune) bool {
		return !unicode.IsLetter(r)
	})

	//将wordvalue加入结果集中
	//切片尾部追加元素append elemnt
	for _,wordvalue := range wordvalues {
		ans = append(ans,mapreduce.KeyValue{wordvalue,"1"})
	}

	return ans

}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func reduceF(key string, values []string) string {
	// Your code here (Part II).
	//进行累加 Itoa()
	//参数key为word，参数values就是[“1”,”1”, …]形式的字符串切片

	var cnt int

	for _,value := range values{
		count,err := strconv.Atoi(value)
		if err != nil {
			log.Fatal("reduceF: strconv string to int error: ", err)
		}
		cnt+=count
	}

	return strconv.Itoa(cnt)





}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100, nil)
	}
}
