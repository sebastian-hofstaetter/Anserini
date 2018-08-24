/**
 * Anserini: An information retrieval toolkit built on Lucene
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.anserini.index;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.*;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.kohsuke.args4j.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class IndexCluewebRelated {
  private static final Logger LOG = LogManager.getLogger(IndexCluewebRelated.class);

  public static final class Args {

    // required arguments
    @Option(name = "-docid2url", metaVar = "[Path]", required = true, usage = "Document Id TO URL mapping file. See: " +
        "http://www.lemurproject.org/clueweb12/related-data.php")
    public String docid2UrlInput;
    
    @Option(name = "-index", metaVar = "[Path]", required = true, usage = "index path")
    public String index;
  
    @Option(name = "-threads", metaVar = "[Number]", required = true, usage = "Number of Threads")
    public int threads;
    
    // optional arguments
    @Option(name = "-pagerank", metaVar = "[Path]", usage = "Pagerank score file. See: " +
        "http://www.lemurproject.org/clueweb09/pageRank.php and http://www.lemurproject.org/clueweb12/PageRank.php")
    public String pagerankInput;
  
    @Option(name = "-spamscore", metaVar = "[Path]", usage = "Spam score file. See: " +
        "https://www.mansci.uwaterloo.ca/~msmucker/cw12spam/")
    public String spamscoreInput;
  
    @Option(name = "-webgraph", metaVar = "[Path]", usage = "Webgraph file. See: " +
        "http://www.lemurproject.org/clueweb09/webGraph.php and http://www.lemurproject.org/clueweb12/webgraph.php/")
    public String webgraphInput;
  
    @Option(name = "-url2nodeid", metaVar = "[Path]", usage = "The Url TO NodeId file for Webgraph. See: " +
        "http://www.lemurproject.org/clueweb09/webGraph.php and http://www.lemurproject.org/clueweb12/webgraph.php/")
    public String url2NodeidInput;
  }
  
  public static final String FIELD_URL = "url";
  public static final String FIELD_PAGERANK = "pagerank";
  public static final String FIELD_PAGERANK_STR = "pagerank_str";
  public static final String FIELD_SPAM = "spam";
  public static final String FIELD_SPAM_STR = "spam_str";
  public static final String FIELD_INLINKS = "inlinks";
  public static final String FIELD_INLINKS_STR = "inlinks_str";
  public static final String FIELD_OUTLINKS = "outlinks";
  public static final String FIELD_OUTLINKS_STR = "outlinks_str";
  private Map<String, Map<String, Object>> values = new HashMap<>();
  
  private InputStream getReadFileStream(String path) throws IOException {
    InputStream fin = Files.newInputStream(Paths.get(path), StandardOpenOption.READ);
    BufferedInputStream in = new BufferedInputStream(fin);
    if (path.endsWith(".bz2")) {
      BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(in);
      return bzIn;
    } else if (path.endsWith(".gz")) {
      GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
      return gzIn;
    } else if (path.endsWith(".zip")) {
      GzipCompressorInputStream zipIn = new GzipCompressorInputStream(in);
      return zipIn;
    }
    return in;
  }
  
  private void readValues() throws IOException {
    // We first need to read the External DocId to URL mapping.
    // This will be used for WebGraph
    Map<String, String> url2DocIdMapping = readUrl2DocIdMapping(args.docid2UrlInput);
    if(args.url2NodeidInput != null && args.webgraphInput != null) {
      Map<Long, String> nodeId2DocIdMapping = readNodeId2DocIdMapping(args.url2NodeidInput, url2DocIdMapping);
      readWebGraph(args.webgraphInput, nodeId2DocIdMapping);
    }
    if(args.pagerankInput != null) {
      readPagerankScores(args.pagerankInput);
    }
    if(args.spamscoreInput != null) {
      readSpamScores(args.spamscoreInput);
    }
  }
  
  /**
   * @param path the input path of the docId2Url file
   * @return Mapping from URL to ExternalDocID
   * @throws IOException
   */
  private Map<String, String> readUrl2DocIdMapping(String path) throws IOException {
    Map<String, String> res = new HashMap<>();
    InputStream in = getReadFileStream(path);
    BufferedReader bRdr = new BufferedReader(new InputStreamReader(in));
    String line;
    while ((line = bRdr.readLine()) != null) {
      String[] lineSplit = line.split("\\s+");
      String docid = lineSplit[0];
      String url = lineSplit[1];
      Map<String, Object> obj = values.getOrDefault(docid, new HashMap<String, Object>());
      obj.put(FIELD_URL, url);
      values.put(docid, obj);
      res.put(url, docid);
    }
    return res;
  }
  
  /**
   * @param path the input path of the Url2NodeId file
   * @param url2DocIdMapping The Mapping of URL to DocId, this should be generated by {@link #readUrl2DocIdMapping}
   * @return Mapping from WebGraph NodeId to External Docid
   * @throws IOException
   */
  private Map<Long, String> readNodeId2DocIdMapping(String path, Map<String, String> url2DocIdMapping) throws IOException {
    Map<Long, String> res = new HashMap<>();
    InputStream in = getReadFileStream(path);
    BufferedReader bRdr = new BufferedReader(new InputStreamReader(in));
    String line;
    while ((line = bRdr.readLine()) != null) {
      String[] lineSplit = line.split("\\s+");
      String url = lineSplit[0];
      long nodeid = Long.parseLong(lineSplit[1]);
      if (url2DocIdMapping.containsKey(url)) {
        res.put(nodeid, url2DocIdMapping.get(url));
      }
    }
    return res;
  }
  
  /**
   * @param path the input path of the Url2NodeId file
   * @param nodeId2DocIdMapping The Mapping of URL to DocId, this should be generated by {@link #readNodeId2DocIdMapping}
   * @return Mapping from WebGraph NodeId to External Docid
   * @throws IOException
   */
  private void readWebGraph(String path, Map<Long, String> nodeId2DocIdMapping) throws IOException {
    InputStream in = getReadFileStream(path);
    BufferedReader bRdr = new BufferedReader(new InputStreamReader(in));
    String line;
    long i = -2;
    while ((line = bRdr.readLine()) != null) {
      i++;
      if ( i == -1 || line.trim().isEmpty() || !nodeId2DocIdMapping.containsKey(i)) continue;
      String docid = nodeId2DocIdMapping.get(i);
      String[] lineSplit = line.split("\\s+");
      int outlinkCount = lineSplit.length;
      Map<String, Object> obj = values.getOrDefault(docid, new HashMap<String, Object>());
      obj.put(FIELD_OUTLINKS, outlinkCount);
      values.put(docid, obj);
      for(String nodeidStr : lineSplit) {
        long nodeid = Long.parseLong(nodeidStr);
        if (nodeId2DocIdMapping.containsKey(nodeid)) {
          String thisDocId = nodeId2DocIdMapping.get(nodeid);
          Map<String, Object> thisObj = values.getOrDefault(thisDocId, new HashMap<String, Object>());
          thisObj.put(FIELD_INLINKS, (int)thisObj.getOrDefault(FIELD_INLINKS,0)+1);
          values.put(thisDocId, thisObj);
        }
      }
    }
  }
  
  /**
   * @param path the input path of the pagerank scores file
   * @throws IOException
   */
  private void readPagerankScores(String path) throws IOException {
    InputStream in = getReadFileStream(path);
    BufferedReader bRdr = new BufferedReader(new InputStreamReader(in));
    String line;
    while ((line = bRdr.readLine()) != null) {
      String[] lineSplit = line.split("\\s+");
      String docid = lineSplit[0];
      double pagerank = Double.parseDouble(lineSplit[1]);
      Map<String, Object> obj = values.getOrDefault(docid, new HashMap<String, Object>());
      obj.put(FIELD_PAGERANK, (double)obj.getOrDefault(FIELD_PAGERANK, 0.0) + pagerank);
      values.put(docid, obj);
    }
  }
  
  /**
   * @param path the input path of the spam scores file
   * @throws IOException
   */
  private void readSpamScores(String path) throws IOException {
    InputStream in = getReadFileStream(path);
    BufferedReader bRdr = new BufferedReader(new InputStreamReader(in));
    String line;
    while ((line = bRdr.readLine()) != null) {
      String[] lineSplit = line.split("\\s+");
      double spam = Double.parseDouble(lineSplit[0]);
      String docid = lineSplit[1];
      Map<String, Object> obj = values.getOrDefault(docid, new HashMap<String, Object>());
      obj.put(FIELD_SPAM, spam);
      values.put(docid, obj);
    }
  }
  
  private final class IndexerThread extends Thread {
    private String docid;
    private Map<String, Object> values;
    final private IndexWriter writer;

    private IndexerThread(IndexWriter writer, String docid, Map<String, Object> values) throws IOException {
      this.docid = docid;
      this.writer = writer;
      this.values = values;
    }

    @Override
    public void run() {
      try {
        // Make a new, empty document.
        Document document = new Document();
  
        // Store the collection docid.
        document.add(new StringField("id", docid, Field.Store.YES));
        // This is needed to break score ties by docid.
        document.add(new SortedDocValuesField("id", new BytesRef(docid)));
  
        for(Map.Entry<String, Object> ent : values.entrySet()) {
          String k = ent.getKey();
          if (k == FIELD_URL) {
            document.add(new StringField(FIELD_URL, (String)ent.getValue(), Field.Store.YES));
          }
          if (k == FIELD_SPAM) {
            document.add(new DoublePoint(FIELD_SPAM, (double)ent.getValue()));
            document.add(new StringField(FIELD_SPAM_STR, ent.getValue().toString(), Field.Store.YES));
          }
          if (k == FIELD_PAGERANK) {
            document.add(new DoublePoint(FIELD_PAGERANK, (double)ent.getValue()));
            document.add(new StringField(FIELD_PAGERANK_STR, ent.getValue().toString(), Field.Store.YES));
          }
          if (k == FIELD_INLINKS) {
            document.add(new IntPoint(FIELD_INLINKS, (int)ent.getValue()));
            document.add(new StringField(FIELD_INLINKS_STR, ent.getValue().toString(), Field.Store.YES));
          }
          if (k == FIELD_OUTLINKS) {
            document.add(new IntPoint(FIELD_OUTLINKS, (int)ent.getValue()));
            document.add(new StringField(FIELD_OUTLINKS_STR, ent.getValue().toString(), Field.Store.YES));
          }
        }
        writer.addDocument(document);
      } catch (Exception e) {
        LOG.error(Thread.currentThread().getName() + ": Unexpected Exception:", e);
      }
    }
  }

  private final IndexCluewebRelated.Args args;
  private final Path indexPath;

  public IndexCluewebRelated(IndexCluewebRelated.Args args) throws Exception {
    this.args = args;
    LOG.info("Index path: " + args.index);
    LOG.info("Threads: " + args.threads);
    LOG.info("docid2UrlInput path: " + args.docid2UrlInput);
    LOG.info("pagerankInput path: " + args.pagerankInput);
    LOG.info("spamscoreInput path: " + args.spamscoreInput);
    LOG.info("webgraphInput path: " + args.webgraphInput);
    LOG.info("url2NodeidInput path: " + args.url2NodeidInput);

    this.indexPath = Paths.get(args.index);
    if (!Files.exists(this.indexPath)) {
      Files.createDirectories(this.indexPath);
    }
  }

  public void run() throws IOException, InterruptedException {
    final long start = System.nanoTime();
    LOG.info("Starting indexer...");

    int numThreads = args.threads;

    final Directory dir = FSDirectory.open(indexPath);
    final IndexWriterConfig config = new IndexWriterConfig();
    config.setSimilarity(new BM25Similarity());
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    config.setRAMBufferSizeMB(2048);
    config.setUseCompoundFile(false);
    config.setMergeScheduler(new ConcurrentMergeScheduler());
    final IndexWriter writer = new IndexWriter(dir, config);
    final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
    readValues();
    for (Map.Entry<String, Map<String, Object>> ent : values.entrySet()) {
      executor.execute(new IndexerThread(writer, ent.getKey(), ent.getValue()));
    }
    executor.shutdown();

    try {
      // Wait for existing tasks to terminate
      while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
        LOG.info(String.format("%.2f percent completed",
            (double) executor.getCompletedTaskCount() / values.size() * 100.0d));
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      executor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }

    int numIndexed = writer.maxDoc();

    try {
      writer.commit();
      writer.forceMerge(1);
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        // It is possible that this happens... but nothing much we can do at this point,
        // so just log the error and move on.
        LOG.error(e);
      }
    }

    final long durationMillis = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    LOG.info(String.format("Total %,d documents indexed in %s", numIndexed,
        DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss")));
  }

  public static void main(String[] args) throws Exception {
    IndexCluewebRelated.Args indexCollectionArgs = new IndexCluewebRelated.Args();
    CmdLineParser parser = new CmdLineParser(indexCollectionArgs, ParserProperties.defaults().withUsageWidth(90));

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println("Example: "+ IndexCluewebRelated.class.getSimpleName() +
          parser.printExample(OptionHandlerFilter.REQUIRED));
      return;
    }

    new IndexCluewebRelated(indexCollectionArgs).run();
  }
}
