package cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.List;
import javax.inject.Singleton;
import models.JobConfig;
import models.JobMetadata;

@Singleton
public class CacheWrapper {
  /**
   * metadataCache
   * - key: job ID (application_[0-9]+_[0-9]+)
   * - value: JobMetadata object containing all the metadata of the jobs (user, status, etc.)
   */
  private static Cache<String, JobMetadata> metadataCache;

  /**
   * configCache
   * - key: job ID (application_[0-9]+_[0-9]+)
   * - value: List of JobConfig objects. Each JobConfig object
   * represents a {@code property} (name-val-source-final) in config.xml
   */
  private static Cache<String, List<JobConfig>> configCache;

  public CacheWrapper() {
    metadataCache = CacheBuilder.newBuilder().build();
    configCache = CacheBuilder.newBuilder().build();
  }

  public static Cache<String, JobMetadata> getMetadataCache() {
    return metadataCache;
  }

  public static Cache<String, List<JobConfig>> getConfigCache() {
    return configCache;
  }
}
