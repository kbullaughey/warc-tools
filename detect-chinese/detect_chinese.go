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
import "unicode/utf8"

var chinese_chars_fn = "detect-chinese/ordered_characters"
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to this file")

//type record []string
type warcRecord struct {
  lines []string
  id string
  body string
}

func interpret(r []string) *warcRecord {
  warc := warcRecord{}
  body := make([]string, 0, 20)
  for _, line := range r {
    if line == "" {
      // Ignore blank lines
      continue
    } else if strings.HasPrefix(line, "WARC") {
      // Metadata
      if line == "WARC-Type: warcinfo" {
        // We can discard warcinfo records
        return nil
      }
      if strings.HasPrefix(line, "WARC-Record-ID: ") {
        warc.id = line[16:]
      }
      continue
    } else if strings.HasPrefix(line, "Content-") {
      // Discard request info
      continue
    } else {
      body = append(body, line)
    }
  }
  // Discard blank records
  if len(body) == 0 { return nil }
  if warc.id == "" { panic("Record missing ID") }
  warc.body = strings.Join(body, " ")
  return &warc
}

func process(r []string, ch chan string) {
  warc := interpret(r)
  threshold := 0.35

  // Nil records can be safely skipped
  if warc == nil {
    ch <- ""
    return
  }

  var n int = 500
  var c float64
  if len(warc.body) <= n {
    n = 0
    // Since we have a short string, we look at all characters
    for _,ru := range warc.body {
      _, ok := chinese_chars[ru]
      if ok { c += 1.0 }
      n++
    }
    ratio := c/float64(n)
    if ratio > threshold {
      ch <- warc.id
    } else {
      ch <- ""
    }
    return
  }

  // Generate random indices uniformly on the number of bytes in the string.
  sample_indices := make([]int, n)
  for i := 0; i < n; i++ {
    sample_indices[i] = rand.Int() % len(warc.body)
  }
  // Sort the indices so we can find them in order as we go through the string
  sort.Ints(sample_indices)

  // Go through and count how many samples are Chinese characters
  var j int // This will keep track of our position in sample_indices
  var m int // Keep track only of unique characters tested.
  for k,ru := range warc.body {
    rune_len := utf8.RuneLen(ru)
    if sample_indices[j] < k + rune_len {
      _, ok := chinese_chars[ru]
      if ok { c += 1.0 }
      m++
      // Increment, skipping duplicates
      for j < n && sample_indices[j] < (k + rune_len) { j++ }
      // If we've found all our samples, break
      if j == n { break }
    }
  }
  if j != n { panic(fmt.Sprintf("Only found %d of %d samples", j, n)) }
  ratio := c/float64(n)
  if ratio > threshold {
//    ch <- strings.Join(r, "\n")
    ch <- warc.id
  } else {
    ch <- ""
  }
  return
}

type runemap map[rune]bool
var chinese_chars runemap

func learnChinese() {
  chinese_chars = make(runemap)
  root := os.Getenv("WARC_TOOLS_DIR")
  if root == "" {
    log.Fatal("Must have WARC_TOOLS_DIR set")
  }
  chinese_chars_path := fmt.Sprintf("%s/%s", root, chinese_chars_fn)
  in, err := os.Open(chinese_chars_path)
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

func launch() {
  /*  Channel to tell printResults (after it starts) how many messages it should
      receive. We only know this after we've scheduled all the goroutines */
  count_ch := make(chan int)

  // Channel for sending strings to printResults.
  ch := make(chan string, 100)

  go printResults(ch, count_ch)
  go readWarc(ch, count_ch)

  // Wait for printing to finish
  <-count_ch
}

func readWarc(ch chan string, count_ch chan int) {
  reader := bufio.NewReader(os.Stdin)
  // start with an empty record.
  var rec []string
  var responses_to_expect int = 0
  for {
    s, err := reader.ReadString('\n')
    if err != nil {
      if err != io.EOF {
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
  launch()
  if *memprofile != "" {
    f, err := os.Create(*memprofile)
    if err != nil { log.Fatal(err) }
    pprof.WriteHeapProfile(f)
    f.Close()
    return
  }
}

// END


