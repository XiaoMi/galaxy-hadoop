package com.xiaomi.infra.galaxy.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectListing;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectSummary;

public class FDSFileSystem extends FileSystem {
  static {
    Configuration.addDefaultResource("galaxy-site.xml");
  }

  public static final Log LOG =
          LogFactory.getLog(FDSFileSystem.class);
  private static final String FOLDER_SUFFIX = "/";
  private static final String LEGACY_FOLDER_SUFFIX = "/_$folder$";

  static class FDSDataInputStream extends FSInputStream {

    private FileSystemStore store;
    private InputStream in;
    private String object;
    private long pos = 0;

    FDSDataInputStream(FileSystemStore store, InputStream in, String object) {
      this.store = store;
      this.in = in;
      this.object = object;
    }

    @Override
    public int read() throws IOException {
      int readCnt = in.read();
      if (readCnt > 0) {
        pos++;
      }
      return readCnt;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int readCnt = in.read(b, off, len);
      if (readCnt > 0) {
        pos += readCnt;
      }
      return readCnt;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public void seek(long pos) throws IOException {
      if (pos < 0) {
        throw new IOException("Invalid seek position: " + pos);
      }
      in.close();
      in = store.getObject(object, pos);
      this.pos = pos;
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }
  }

  static class FDSDataOutputStream extends OutputStream {
    private static final int STREAM_BUFFER_SIZE = 4096;
    private FileSystemStore store;
    private final String object;

    private boolean closed;
    private Thread writerThread;
    private PipedInputStream inputStream;
    private PipedOutputStream outputStream;

    private volatile IOException lastException;

    /**
     * Use piped stream and writer thread to simulate stream write.
     * Main thread ==> pipedOutputsteam ==> pipedInputStream ==> writerThread
     */
    private class Writer implements Runnable {
      @Override
      public void run() {
        try {
          store.putObject(object, inputStream, null);
        } catch (IOException e) {
          lastException = e;
        }
      }
    }

    public FDSDataOutputStream(FileSystemStore store, String object) throws IOException {
      this.store = store;
      this.object = object;
      initialize();
    }

    private void initialize() throws IOException {
      inputStream = new PipedInputStream(STREAM_BUFFER_SIZE);
      outputStream = new PipedOutputStream(inputStream);
      writerThread = new Thread(new Writer());
      writerThread.start();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (lastException != null) {
        throw lastException;
      }

      outputStream.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
      if (lastException != null) {
        throw lastException;
      }
      outputStream.write(b);
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }

      outputStream.flush();
      outputStream.close();
      if (writerThread != null) {
        try {
          writerThread.join();
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
      closed = true;
      if (lastException != null) {
        throw lastException;
      }
    }
  }

  private URI uri;
  private Path workingDir;
  private FDSFileSystemStore store;

  public FDSFileSystem() {
  }

  @Override
  public String getScheme() {
    return "fds";
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getRawAuthority());
    this.workingDir =
            new Path("/user", System.getProperty("user.name")).makeQualified(this);
    store = new FDSFileSystemStore();
    store.initialize(uri, conf);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    FileStatus fileStatus = getFileStatus(path);
    if (fileStatus.isDirectory()) {
      throw new IOException("'" + path + "' is a directory");
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening '" + path + "' for reading");
    }

    Path absolutePath = makeAbsolute(path);
    String object = pathToObject(absolutePath);
    return new FSDataInputStream(new BufferedFSInputStream(
            new FDSDataInputStream(store, store.getObject(object, 0), object), bufferSize));
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize,
                                   Progressable progressable) throws IOException {
    if (exists(path)) {
      if (overwrite) {
        delete(path, true);
      } else {
        throw new IOException("File already exists:" + path);
      }
    }
    Path parent = path.getParent();
    if (!exists(parent)) {
      // The parents folder markers should be created, see S3 implementation
      mkdirs(parent);
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("Creating new file '" + path + "' in FDS");
    }

    Path absolutePath = makeAbsolute(path);
    String object = pathToObject(absolutePath);
    return new FSDataOutputStream(new FDSDataOutputStream(store, object));
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    throw new IOException("FDSFileSystem:append() not supported");
  }

  // WARNING: No atomicity is guaranteed for this operation
  @Override
  public boolean rename(Path srcPath, Path dstPath) throws IOException {
    if (srcPath.isRoot()) {
      LOG.error("Rename error, source cannot be root");
      return false;
    }

    // Not test whether desPath exists.
    // Cases      src Type      dst Type       Result
    // case 1        file         file          overwrite
    // case 2        file         dir              N/A
    // case 3        dir          dir              Merge, and overwrite file of the same file name
    // case 4        dir          file             N/A

    FileStatus srcStatus;
    FileStatus dstStatus;
    try {
      srcStatus = getFileStatus(srcPath);
    } catch (FileNotFoundException e) {
      LOG.error("Rename error, source not exists " + srcPath);
      return false;
    }

    try {
      dstStatus = getFileStatus(dstPath);
      if (srcStatus.isDirectory() != dstStatus.isDirectory()) {
        throw new IOException("Rename error, destination exists and is of different type with source," +
            " source is dir: " + srcStatus.isDirectory() +
            " destination is dir: " + dstStatus.isDirectory());
      }
    } catch (FileNotFoundException e) {
      LOG.debug("Rename's destination not exists, destination not exists " + dstPath);
    }

    Path srcAbsolutePath = makeAbsolute(srcPath);
    Path dstAbsolutePath = makeAbsolute(dstPath);
    String srcObject = pathToObject(srcAbsolutePath);
    String dstObject = pathToObject(dstAbsolutePath);

    if (srcStatus.isFile()) {
      store.rename(srcObject, dstObject);
    } else {
      if (dstPath.getParent().toString().startsWith(srcPath.toString())) {
        LOG.error("Rename error, because destination is subdirectory of soure path");
        return false;
      }
      renameDir(srcAbsolutePath, dstAbsolutePath);
    }

    Path parent = srcAbsolutePath.getParent();
    // make sure parent exist, make it explicityly
    mkdir(parent);

    return true;
  }

  private void renameDir(Path srcPath, Path dstPath) throws IOException {
    String srcObject = pathToObject(srcPath);
    URI srcPathUri = makeQualified(srcPath).toUri();
    FDSObjectListing listing = null;
    do {
      // Not using directory delimeter, and rename all sub objects in srcPath
      listing = store.listSubPaths(srcObject, listing, "");
      if (listing == null) {
        break;
      }

      for (FDSObjectSummary fdsObjectSummary : listing.getObjectSummaries()) {
        Path srcFilePath = makeQualified(objectToPath(fdsObjectSummary.getObjectName()));
        String child = srcPathUri.relativize(srcFilePath.toUri()).getPath();
        Path dstFilePath = child.isEmpty() ? dstPath : new Path(dstPath, child);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Rename file path src: " + srcFilePath + " dst: " + dstFilePath);
        }
        if (fdsObjectSummary.getObjectName().endsWith(FOLDER_SUFFIX)) {
          store.rename(fdsObjectSummary.getObjectName(), pathToObject(dstFilePath) + FOLDER_SUFFIX);
        } else {
          store.rename(fdsObjectSummary.getObjectName(), pathToObject(dstFilePath));
        }
      }

    } while (listing.isTruncated());
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    Path absolutePath = makeAbsolute(path);
    String object = pathToObject(path);
    FileStatus fileStatus;
    try {
      fileStatus = getFileStatus(path);
    } catch (FileNotFoundException e) {
      LOG.warn("Delete error, path not exists: " + path);
      return false;
    }

    if (fileStatus.isFile()) {
      store.delete(object);
    } else if (fileStatus.isDirectory()) {
      if (!recursive) {
        LOG.warn("Can't delete directory " + path + " while not set recursive" +
                " as true");
        return false;
      }

      deleteRecursively(absolutePath);
    }
    // make sure parent exist, make it explicityly
    Path parent = absolutePath.getParent();
    mkdir(parent);
    return true;
  }

  private void deleteRecursively(Path path) throws IOException {
    String object = pathToObject(path);
    FileStatus[] fileStatuses = listStatus(path);
    for (FileStatus status : fileStatuses) {
      if (status.isDirectory()) {
        deleteRecursively(status.getPath());
      } else {
        store.delete(pathToObject(status.getPath()));
      }
    }

    try {
      store.delete(object + FOLDER_SUFFIX);
    } catch (IOException ioe) {
      if (ioe.toString().contains("Object Not Found") ||
          ioe.toString().contains("status=404")) {
        store.delete(object + LEGACY_FOLDER_SUFFIX);
      } else {
        throw ioe;
      }
    }
  }

  @Override
  public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
    Path absolutePath = makeAbsolute(path);
    String object = pathToObject(absolutePath);

    List<FileStatus> fileList = new LinkedList<FileStatus>();

    FDSObjectListing listing = null;
    boolean isDir = false;
    do {
      listing = store.listSubPaths(object, listing);
      if (listing == null) {
        break;
      }

      for (FDSObjectSummary fdsObjectSummary : listing.getObjectSummaries()) {
        String objectName = fdsObjectSummary.getObjectName();
        if (objectName.equals(object + FOLDER_SUFFIX) ||
            objectName.equals(object + LEGACY_FOLDER_SUFFIX)) {
          // placeholder for directory itself
          isDir = true;
          continue;
        }

        fileList.add(newFile(
                new FileMetadata(objectToPath(objectName).toString(),
                    fdsObjectSummary.getSize(), 0),
                objectToPath(objectName))
        );
      }

      for (String commonPrefix : listing.getCommonPrefixes()) {
        fileList.add(newDirectory(objectToPath(commonPrefix)));
      }

    } while (listing.isTruncated());

    if (!isDir && fileList.isEmpty()) {
      throw new FileNotFoundException();
    }

    return fileList.toArray(new FileStatus[fileList.size()]);
  }

  @Override
  public void setWorkingDirectory(Path path) {
    this.workingDir = path;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    Path absolutePath = makeAbsolute(path);
    List<Path> paths = new ArrayList<Path>();
    do {
      paths.add(0, absolutePath);
      absolutePath = absolutePath.getParent();
    } while (absolutePath != null);

    boolean result = true;
    for (Path p : paths) {
      result &= mkdir(p);
      if (!result) {
        break;
      }
    }
    return result;
  }

  private boolean mkdir(Path path) throws IOException {
    try {
      FileStatus fileStatus = getFileStatus(path);
      if (fileStatus.isFile()) {
        throw new IOException(String.format(
                "Can't make directory for path '%s' since it is a file.",
                path));
      }
    } catch (FileNotFoundException e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Making dir '" + path + "' in FDS");
      }
      String object = pathToObject(path) + FOLDER_SUFFIX;
      store.storeEmptyFile(object);
    }
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    Path absolutePath = makeAbsolute(path);
    String object = pathToObject(path);

    if (object.length() == 0) { // root always exists
      return newDirectory(absolutePath);
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("getFileStatus getting metadata for object '" + object+ "'");
    }

    FileMetadata meta = store.getMetadata(object);
    if (meta != null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("getFileStatus returning 'file' for object '" + object +
                "'");
      }
      return newFile(meta, absolutePath);
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("getFileStatus getting metadata for object '" + object +
              FOLDER_SUFFIX + "' or '" + LEGACY_FOLDER_SUFFIX + "'");
    }

    // test directory placeholder
    if (store.getMetadata(object + FOLDER_SUFFIX) != null ||
        store.getMetadata(object + LEGACY_FOLDER_SUFFIX) != null) {
      return newDirectory(absolutePath);
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("getFileStatus get listing for object '" + object+ "'");
    }

    // test implicit directory(without directory placeholder)
    FDSObjectListing listing = store.listSubPaths(object);
    if (listing != null &&
        (!listing.getObjectSummaries().isEmpty()
        || !listing.getCommonPrefixes().isEmpty())) {
      return newDirectory(absolutePath);
    }

    if(LOG.isDebugEnabled()) {
      LOG.debug("getFileStatus could not find path '" + absolutePath + "'");
    }

    throw new FileNotFoundException("No such file or directory '" +
            absolutePath + "'");
  }

  private FileStatus newFile(FileMetadata meta, Path path) {
    return new FileStatus(meta.getLength(), false, 1, getDefaultBlockSize(),
            meta.getLastModified(), path.makeQualified(this));
  }

  private FileStatus newDirectory(Path path) {
    return new FileStatus(0, true, 1, 0, 0, path.makeQualified(this));
  }

  private Path objectToPath(String objectName) {
    return new Path("/", objectName);
  }

  private String pathToObject(Path path) {
    if (path.toUri().getScheme() != null && "".equals(path.toUri().getPath())) {
      // allow uris without trailing slash after bucket to refer to root,
      // like fds://mybucket
      return "";
    }
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }
    String ret = path.toUri().getPath().substring(1); // remove initial slash
    if (ret.endsWith("/") && (ret.indexOf("/") != ret.length() - 1)) {
      ret = ret.substring(0, ret.length() -1);
    }
    return ret;
  }

  @Override
  public String getCanonicalServiceName() {
    // To pass obtain token process for MapReduce
    return getUri().getAuthority();
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    return;
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    return;
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    return;
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    return;
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    return;
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    return new AclStatus.Builder().build();
  }
}
