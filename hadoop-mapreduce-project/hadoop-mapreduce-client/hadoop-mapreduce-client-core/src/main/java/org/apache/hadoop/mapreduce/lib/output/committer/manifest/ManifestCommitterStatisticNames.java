/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.StoreStatisticNames;

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_RENAME;

/**
 * Statistic names for committers.
 * Please keep in sync with org.apache.hadoop.fs.s3a.Statistic
 * so that S3A and manifest committers are in sync.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class ManifestCommitterStatisticNames {


  /** Amount of data committed: {@value}. */
  public static final String COMMITTER_BYTES_COMMITTED_COUNT =
      "committer_bytes_committed";

  /** Duration Tracking of time to commit an entire job: {@value}. */
  public static final String COMMITTER_COMMIT_JOB =
      "committer_commit_job";

  /** Number of files committed: {@value}. */
  public static final String COMMITTER_FILES_COMMITTED_COUNT =
      "committer_files_committed";

  /** "Count of successful tasks:: {@value}. */
  public static final String COMMITTER_TASKS_COMPLETED_COUNT =
      "committer_tasks_completed";

  /** Count of failed tasks: {@value}. */
  public static final String COMMITTER_TASKS_FAILED_COUNT =
      "committer_tasks_failed";

  /** Count of commits aborted: {@value}. */
  public static final String COMMITTER_COMMITS_ABORTED_COUNT =
      "committer_commits_aborted";

  /** Count of commits reverted: {@value}. */
  public static final String COMMITTER_COMMITS_REVERTED_COUNT =
      "committer_commits_reverted";

  /** Count of commits failed: {@value}. */
  public static final String COMMITTER_COMMITS_FAILED =
      "committer_commits" + StoreStatisticNames.SUFFIX_FAILURES;

  /**
   * The number of files in a task. This will be a MeanStatistic.
   */
  public static final String COMMITTER_FILE_COUNT_MEAN =
      "committer_task_file_count";

  /**
   * File Size.
   */
  public static final String COMMITTER_FILE_SIZE_MEAN =
      "committer_task_file_size";

  /**
   * What is a task attempt's directory count.
   */
  public static final String COMMITTER_TASK_DIRECTORY_COUNT_MEAN =
      "committer_task_directory_count";

  /**
   * What is the depth of a task attempt's directory tree.
   */
  public static final String COMMITTER_TASK_DIRECTORY_DEPTH_MEAN =
      "committer_task_directory_depth";

  /**
   * The number of files in a task. This will be a MeanStatistic.
   */
  public static final String COMMITTER_TASK_FILE_COUNT_MEAN =
      "committer_task_file_count";

  /**
   * The number of files in a task. This will be a MeanStatistic.
   */
  public static final String COMMITTER_TASK_FILE_SIZE_MEAN =
      "committer_task_file_size";

  /**
   * The size of manifest files. This will be a MeanStatistic.
   * Useful to highlight whether this size is too large and
   * effort to optimise for file IO and memory consumption
   * justified.
   */
  public static final String COMMITTER_TASK_MANIFEST_FILE_SIZE =
      "committer_task_manifest_file_size";

  /**
   * A file renamed during the commit operation {@value}.
   */
  public static final String OP_COMMIT_FILE_RENAME =
      "commit_file_rename";

  /**
   * A file rename during the commit operation was recovered from a failure {@value}.
   */
  public static final String OP_COMMIT_FILE_RENAME_RECOVERED =
      "commit_file_rename_recovered";

  /** Directory creation {@value}. */
  public static final String OP_CREATE_DIRECTORIES = "op_create_directories";

  /** Creating a single directory {@value}. */
  public static final String OP_CREATE_ONE_DIRECTORY =
      "op_create_one_directory";

  /**
   * A file in the destination directory tree has been deleted.
   *  {@value}.
   */
  public static final String OP_DELETE_FILE_UNDER_DESTINATION =
      "op_delete_file_under_destination";

  /** Directory scan {@value}. */
  public static final String OP_DIRECTORY_SCAN = "op_directory_scan";

  /**
   * Overall job commit {@value}.
   */
  public static final String OP_STAGE_JOB_COMMIT = COMMITTER_COMMIT_JOB;

  /** {@value}. */
  public static final String OP_LOAD_ALL_MANIFESTS = "op_load_all_manifests";

  /**
   * Load a task manifest: {@value}.
   */
  public static final String OP_LOAD_MANIFEST = "op_load_manifest";

  /**
   * mkdir failure statistic: {@value}.
   * This is incremented whenever mkdir() returns false, e.g because
   * there was a file at the end of the path.
   */
  public static final String OP_MKDIRS_RETURNED_FALSE = "op_mkdir_returned_false";

  /**
   * msync statistic: {@value}.
   * This should be the same as StoreStatisticNames.OP_MSYNC; it's
   * duplicated just to isolate this committer into a single JAR
   * for ease of testing.
   */
  public static final String OP_MSYNC = "op_msync";

  /**
   * Preparing ancestor dirs: {@value}.
   * Probing for paths being files, and if true: deleting them.
   */
  public static final String OP_PREPARE_DIR_ANCESTORS = "op_prepare_dir_ancestors";

  /** Rename a dir: {@value}. */
  public static final String OP_RENAME_DIR = OP_RENAME;


  /** Rename a file: {@value}. */
  public static final String OP_RENAME_FILE = OP_RENAME;

  /**
   * Save a task manifest: {@value}.
   */
  public static final String OP_SAVE_TASK_MANIFEST =
      "task_stage_save_task_manifest";

  /**
   * Task abort: {@value}.
   */
  public static final String OP_STAGE_TASK_ABORT_TASK
      = "task_stage_abort_task";

  /**
   * Job abort: {@value}.
   */
  public static final String OP_STAGE_JOB_ABORT = "job_stage_abort";

  /**
   * Job cleanup: {@value}.
   */
  public static final String OP_STAGE_JOB_CLEANUP = "job_stage_cleanup";

  /**
   * Prepare Directories Stage: {@value}.
   */
  public static final String OP_STAGE_JOB_CREATE_TARGET_DIRS =
      "job_stage_create_target_dirs";

  /**
   * Load Manifest Stage: {@value}.
   */
  public static final String OP_STAGE_JOB_LOAD_MANIFESTS =
      "job_stage_load_manifests";

  /**
   * Rename files stage duration: {@value}.
   */
  public static final String OP_STAGE_JOB_RENAME_FILES =
      "job_stage_rename_files";


  /**
   * Job Setup Stage: {@value}.
   */
  public static final String OP_STAGE_JOB_SETUP = "job_stage_setup";

  /**
   * Job saving _SUCCESS marker Stage: {@value}.
   */
  public static final String OP_STAGE_JOB_SAVE_SUCCESS =
      "job_stage_save_success_marker";

  /**
   * Output Validation (within job commit) Stage: {@value}.
   */
  public static final String OP_STAGE_JOB_VALIDATE_OUTPUT =
      "job_stage_optional_validate_output";

  /**
   * Task saving manifest file Stage: {@value}.
   */
  public static final String OP_STAGE_TASK_SAVE_MANIFEST =
      "task_stage_save_manifest";

  /**
   * Task Setup Stage: {@value}.
   */
  public static final String OP_STAGE_TASK_SETUP = "task_stage_setup";

  /**
   * Task Commit Stage: {@value}.
   */
  public static final String OP_STAGE_TASK_COMMIT = "task_stage_commit";

  /** Task Scan directory Stage: {@value}. */
  public static final String OP_STAGE_TASK_SCAN_DIRECTORY
      = "task_stage_scan_directory";

  private ManifestCommitterStatisticNames() {
  }
}
