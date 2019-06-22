/**
 * Anserini: A toolkit for reproducible information retrieval research built on Lucene
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

package io.anserini.collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * A .tsv collection.
 * We assume the following .tsv structure for every line == 1 doc with: id\tcontent 
 */
public class TsvCollection extends DocumentCollection
    implements SegmentProvider<TsvCollection.Document> {

  private static final Logger LOG = LogManager.getLogger(TsvCollection.class);

  @Override
  public List<Path> getFileSegmentPaths() {
    Set<String> allowedFileSuffix = new HashSet<>(Arrays.asList(".tsv"));

    return discover(path, EMPTY_SET, EMPTY_SET, EMPTY_SET,
        allowedFileSuffix, EMPTY_SET);
  }

  @Override
  public FileSegment createFileSegment(Path p) throws IOException {
    return new FileSegment(p);
  }

  public class FileSegment extends BaseFileSegment<Document> {

    protected FileSegment(Path path) throws IOException {
      this.path = path;
      String fileName = path.toString();
      this.bufferedReader = new BufferedReader(new FileReader(fileName));

    }

    @Override
    public void readNext() throws IOException {
      String line = this.bufferedReader.readLine();

      if(line == null){
        this.atEOF = true;
        bufferedReader = null;
        return;
      }

      String[] line_split = line.split("\t");

      if(line_split.length != 2){
        throw new IOException(".tsv file has the wrong file format:"+line_split.length +" for: "+line);
      }

      bufferedRecord = new Document(line_split[0], line_split[1]);
    }
  }

  /**
   * A Wikipedia document. The article title serves as the id.
   */
  public static class Document implements SourceDocument {
    private final String title;
    private final String contents;

    public Document(String title, String contents) {
      this.title = title;
      this.contents = contents;
    }

    @Override
    public String id() {
      return title;
    }

    @Override
    public String content() {
      return contents;
    }

    @Override
    public boolean indexable() {
      return true;
    }
  }
}
