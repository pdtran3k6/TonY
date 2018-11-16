package controllers;

import cache.CacheWrapper;
import com.google.common.cache.Cache;
import com.typesafe.config.Config;
import hadoop.Configuration;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import models.JobMetadata;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import play.Logger;
import play.Logger.ALogger;
import play.mvc.Controller;
import play.mvc.Result;
import utils.HdfsUtils;

import static utils.HdfsUtils.*;
import static utils.ParserUtils.*;


public class JobsMetadataPageController extends Controller {
  private static final ALogger LOG = Logger.of(JobsMetadataPageController.class);
  private final Config config;

  @Inject
  public JobsMetadataPageController(Config config) {
    this.config = config;
  }

  public Result index() {
    HdfsConfiguration conf = Configuration.getHdfsConf();
    FileSystem myFs = HdfsUtils.getFileSystem(conf);
    Cache<String, JobMetadata> cache = CacheWrapper.getMetadataCache();

    if (myFs == null) {
      return internalServerError("Failed to initialize file system");
    }

    List<JobMetadata> listOfMetadata = new ArrayList<>();
    Path tonyHistoryFolder = new Path(config.getString("tony.historyFolder"));
    String jobFolderRegex = "^application_\\d+_\\d+$";
    JobMetadata tmpMetadata;
    String jobId;

    for (Path f : getJobFolders(myFs, tonyHistoryFolder, jobFolderRegex)) {
      jobId = getJobId(f.toString());
      if (cache.asMap().containsKey(jobId)) {
        tmpMetadata = cache.getIfPresent(jobId);
        listOfMetadata.add(tmpMetadata);
        continue;
      }
      try {
        tmpMetadata = parseMetadata(myFs, f, jobFolderRegex);
        cache.put(jobId, tmpMetadata);
        listOfMetadata.add(tmpMetadata);
      } catch (Exception e) {
        LOG.error("Couldn't parse " + f, e);
      }
    }

    return ok(views.html.metadata.render(listOfMetadata));
  }
}