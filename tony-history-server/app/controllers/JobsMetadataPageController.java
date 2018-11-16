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
    String appId;

    for (Path f : getJobFolders(myFs, tonyHistoryFolder, jobFolderRegex)) {
      appId = getJobId(f.toString());
      if (cache.asMap().containsKey(appId)) {
        LOG.info("Get from cache");
        tmpMetadata = cache.getIfPresent(appId);
      } else {
        tmpMetadata = parseMetadata(myFs, f, jobFolderRegex);
        cache.put(appId, tmpMetadata);
      }
      if (tmpMetadata == null) {
        LOG.error("Couldn't parse " + f);
        continue;
      }
      listOfMetadata.add(tmpMetadata);
    }

    return ok(views.html.metadata.render(listOfMetadata));
  }
}