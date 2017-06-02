package io.anserini.kg.freebase;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene62.Lucene62Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.kohsuke.args4j.ParserProperties;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Builds a triples lookup index from a Freebase dump in N-Triples RDF format. Each
 * {@link ObjectTriples} object, which represents a group of triples that share the same subject,
 * is treated as a Lucene "document". This class builds an index primarily for lookup by
 * <code>mid</code>.
 */
public class IndexObjectTriples {
  private static final Logger LOG = LogManager.getLogger(IndexObjectTriples.class);

  public static final class Args {
    // Required arguments

    @Option(name = "-input", metaVar = "[directory]", required = true, usage = "collection directory")
    public String input;

    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    public String index;

    // Optional arguments

    @Option(name = "-predicates", handler = StringArrayOptionHandler.class, usage = "predicates to index")
    public List<String> predicatesToIndex;
  }

  public final class Counters {
    public AtomicLong indexedDocuments = new AtomicLong();
  }

  /**
   * Program arguments to hold parameters and properties.
   */
  private Args args;

  private final Path indexPath;
  private final Path collectionPath;
  private final Counters counters;

  /**
   * Constructor
   *
   * @param args program arguments
   * @throws Exception
   */
  public IndexObjectTriples(Args args) throws Exception {

    // Copy arguments
    this.args = args;

    // Log parameters
    LOG.info("Collection path: " + args.input);
    LOG.info("Index path: " + args.index);
    if (args.predicatesToIndex != null && args.predicatesToIndex.size() > 0) {
      LOG.info("Predicates to index: " + String.join(",", args.predicatesToIndex));
    } else {
      LOG.info("Predicates to index: None");
    }

    // Initialize variables
    this.indexPath = Paths.get(args.index);
    if (!Files.exists(this.indexPath)) {
      Files.createDirectories(this.indexPath);
    }

    collectionPath = Paths.get(args.input);
    if (!Files.exists(collectionPath) || !Files.isReadable(collectionPath)) {
      throw new IllegalArgumentException("Document file/directory " + collectionPath.toString() +
              " does not exist or is not readable, please check the path");
    }

    this.counters = new Counters();
  }

  private void run() throws IOException, InterruptedException {
    final long start = System.nanoTime();
    LOG.info("Starting indexer...");

    final Directory dir = FSDirectory.open(indexPath);
    final EnglishAnalyzer analyzer = new EnglishAnalyzer();
    final IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    config.setCodec(new Lucene62Codec(Lucene50StoredFieldsFormat.Mode.BEST_SPEED));
    config.setUseCompoundFile(false);

    final IndexWriter writer = new IndexWriter(dir, config);
    index(writer, collectionPath);

    int numIndexed = writer.maxDoc();
    try {
      writer.commit();
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        LOG.error(e);
      }
    }

    LOG.info("Indexed documents: " + counters.indexedDocuments.get());
    final long durationMillis = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    LOG.info("Total " + numIndexed + " documents indexed in " +
            DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss"));
  }

  /**
   * Run the indexing process.
   *
   * @param writer index writer
   * @param inputFile which file to index from the collection
   * @throws IOException on error
   */
  private void index(IndexWriter writer, Path inputFile) throws IOException {
    ObjectTriplesLuceneDocumentGenerator transformer = new ObjectTriplesLuceneDocumentGenerator();
    transformer.config(args);
    transformer.setCounters(counters);

    int cnt = 0;
    ObjectTriplesIterator iter = new ObjectTriplesIterator(inputFile);
    while (iter.hasNext()) {
      ObjectTriples d = iter.next();
      Document doc = transformer.createDocument(d);

      if (doc != null) {
        writer.addDocument(doc);
        cnt++;
      }

      // Display progress
      if (cnt % 100000 == 0) {
        LOG.debug("Number of indexed entity document: {}", cnt);
      }

      d = null;
      doc = null;
    }

    iter.close();
    LOG.info(inputFile.getParent().getFileName().toString() + File.separator +
            inputFile.getFileName().toString() + ": " + cnt + " docs added.");
    counters.indexedDocuments.addAndGet(cnt);
  }

  public static void main(String[] args) throws Exception {
    Args indexRDFCollectionArgs = new Args();
    CmdLineParser parser = new CmdLineParser(indexRDFCollectionArgs,
            ParserProperties.defaults().withUsageWidth(90));

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println("Example command: "+ IndexObjectTriples.class.getSimpleName() +
              parser.printExample(OptionHandlerFilter.REQUIRED));
      return;
    }

    new IndexObjectTriples(indexRDFCollectionArgs).run();
  }
}