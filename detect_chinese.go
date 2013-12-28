package main

import "fmt"
import "os"
import "io"
import "bufio"
import "strings"
import "math/rand"
import "sort"
import "flag"
import "log"
import "runtime/pprof"

var chinese_chars_fn = "ordered_characters"
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to this file")

//type record []string

func recordBody(r []string) *[]string {
  body := make([]string, 0, 20)
  for _, line := range r {
    if line == "" {
      // Ignore blank lines
      continue
    } else if len(line) >= 4 && line[:4] == "WARC" {
      // Metadata
      if line == "WARC-Type: warcinfo" {
        // We can discard warcinfo records
        return nil
      }
      continue
    } else if len(line) >= 8 && line[:8] == "Content-" {
      // Discard request info
      continue
    } else {
      body = append(body, line)
    }
  }
  return &body
}

func process(r []string, ch chan string) {
  body := recordBody(r)
  threshold := 0.3

  // Nil records can be safely skipped
  if body == nil {
    ch <- ""
    return
  }

  /* Go through the string and make an array of the runes, as we want to
     operate on characters. */
  body_str := strings.Join(*body, " ")
  runes := make([]rune, 0, 100)
  for _, ru := range body_str {
    runes = append(runes, ru)
  }

  var n int = 200
  var c float64
  if len(runes) <= n {
    n = len(runes)
    // Since we have at most n runes, we just use them all
    for _,ru := range runes {
      _, ok := chinese_chars[ru]
      if ok {
        c += 1.0
      }
    }
    ratio := c/float64(n)
    if ratio > threshold {
      ch <- body_str
    } else {
      ch <- ""
    }
    return
  }

  // Generate random indices uniformly on the rune slice length
  sample_indices := make([]int, n)
  for i := 0; i < n; i++ {
    sample_indices[i] = rand.Int() % len(runes)
  }
  // Sort the indices so we can find them in order as we go through the string
  sort.Ints(sample_indices)

  // Go through and count how many samples are Chinese characters
  for _, idx := range sample_indices {
    ru := runes[idx]
    _, ok := chinese_chars[ru]
    if ok {
      c += 1.0
    }
  }
  ratio := c/float64(n)
  if ratio > threshold {
    ch <- body_str
  } else {
    ch <- ""
  }
  return
}

type runemap map[rune]bool
var chinese_chars runemap

func learnChinese() {
  chinese_chars = make(runemap)
  in, err := os.Open(chinese_chars_fn)
  if err != nil { panic(err) }
  // close file on exit and check for its returned error
  defer func() {
    if err := in.Close(); err != nil {
      panic(err)
    }
  }()
  reader := bufio.NewReader(in)
  for {
    s, err := reader.ReadString('\n')
    if err != nil {
      if err == io.EOF {
        break
      }
      panic(err)
    }
    s = strings.TrimSpace(s)
    for _, ru := range s {
      chinese_chars[ru] = true
      // I only care about the first rune
      break
    }
  }
}

func printResults(ch chan string, count_ch chan int) {
  // We should receive one result per goroutine
  var expecting int
  var received_count int = 1
  for {
    select {
      case response := <-ch:
        if response != "" {
          fmt.Println("")
          fmt.Println(response)
        }
        received_count += 1
      case expecting = <-count_ch:
    }
    if expecting > 0 && received_count == expecting {
      break
    }
  }
  // Indicate we're done.
  count_ch <- 0
}

func readWarc() {
  /*  Channel to tell printResults (after it starts) how many messages it should
      receive. We only know this after we've scheduled all the goroutines */
  count_ch := make(chan int)

  // Channel for sending strings to printResults.
  ch := make(chan string, 100)

  // Launch the printResults goroutine.
  go printResults(ch, count_ch)

  reader := bufio.NewReader(os.Stdin)
  // start with an empty record.
  var rec []string
  var responses_to_expect int = 0
  for {
    s, err := reader.ReadString('\n')
    if err != nil {
      if err == io.EOF {
        fmt.Fprintln(os.Stderr, "Reached EOF")
      } else {
        fmt.Fprintln(os.Stderr, "ReadString error: ", err)
      }
      break
    }
    s = strings.TrimSpace(s)
    if s == "WARC/1.0" {
      /* Process the record. If the fraction of Chinese is high enough, then
         print it out. Reset the record afterwards. */
      go process(rec, ch)
      responses_to_expect++
      rec = make([]string, 0, 20)
    }
    rec = append(rec, s)
  }

  // tell printResults how many messages it should receive
  count_ch <- responses_to_expect
  // Wait for printing to finish
  <-count_ch
  return
}

func main() {
  flag.Parse()
  if *cpuprofile != "" {
    f, err := os.Create(*cpuprofile)
    if err != nil { log.Fatal(err) }
    pprof.StartCPUProfile(f)
    defer pprof.StopCPUProfile()
  }
  learnChinese()
  readWarc()
  if *memprofile != "" {
    f, err := os.Create(*memprofile)
    if err != nil { log.Fatal(err) }
    pprof.WriteHeapProfile(f)
    f.Close()
    return
  }
}

// END


