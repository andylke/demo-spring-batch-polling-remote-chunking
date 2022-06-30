package com.github.andylke.demo.support;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.repository.dao.AbstractJdbcBatchMetadataDao;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.RowMapper;

public class JdbcRemoteChunkExecutionDao extends AbstractJdbcBatchMetadataDao
    implements RemoteChunkExecutionDao, InitializingBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRemoteChunkExecutionDao.class);

  private static final String REMOTE_CHUNK_EXECUTION_TABLE =
      "%PREFIX%REMOTE_CHUNK_EXECUTION_%SUFFIX%";

  private static final String SAVE_REMOTE_CHUNK_EXECUTION =
      "INSERT INTO "
          + REMOTE_CHUNK_EXECUTION_TABLE
          + " (STEP_EXECUTION_ID, SEQUENCE, START_TIME, END_TIME, STATUS,"
          + " ITEM_COUNT, COMPLETED_COUNT, SENT_COUNT, RECEIVED_COUNT,"
          + " EXIT_CODE, EXIT_MESSAGE, LAST_UPDATED)"
          + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String UPDATE_REMOTE_CHUNK_EXECUTION =
      "UPDATE "
          + REMOTE_CHUNK_EXECUTION_TABLE
          + " SET"
          + " START_TIME = ?, END_TIME = ?, STATUS = ?,"
          + " ITEM_COUNT = ?, COMPLETED_COUNT = ?, SENT_COUNT = ?, RECEIVED_COUNT = ?,"
          + " EXIT_CODE = ?, EXIT_MESSAGE = ?, LAST_UPDATED = ?"
          + " WHERE STEP_EXECUTION_ID = ? AND SEQUENCE = ?";

  private static final String GET_REMOTE_CHUNK_EXECUTION =
      "SELECT"
          + " STEP_EXECUTION_ID, SEQUENCE, START_TIME, END_TIME, STATUS,"
          + " ITEM_COUNT, COMPLETED_COUNT, SENT_COUNT, RECEIVED_COUNT,"
          + " EXIT_CODE, EXIT_MESSAGE, LAST_UPDATED"
          + " FROM "
          + REMOTE_CHUNK_EXECUTION_TABLE
          + " WHERE STEP_EXECUTION_ID = ? AND SEQUENCE = ?";

  private static final String COUNT_REMOTE_CHUNK_EXECUTION_BY_STATUS =
      "SELECT COUNT(*) FROM "
          + REMOTE_CHUNK_EXECUTION_TABLE
          + " WHERE STEP_EXECUTION_ID = ? AND STATUS = ?";

  private int exitMessageLength = DEFAULT_EXIT_MESSAGE_LENGTH;

  public JdbcRemoteChunkExecutionDao(JdbcOperations jdbcTemplate) {
    setJdbcTemplate(jdbcTemplate);
  }

  @Override
  public void saveRemoteChunkExecution(
      String tableSuffix, RemoteChunkExecution remoteChunkExecution) {
    Object[] parameterValues =
        new Object[] {
          remoteChunkExecution.getStepExecutionId(),
          remoteChunkExecution.getSequence(),
          remoteChunkExecution.getStartTime(),
          remoteChunkExecution.getEndTime(),
          remoteChunkExecution.getStatus(),
          remoteChunkExecution.getItemCount(),
          remoteChunkExecution.getCompletedCount(),
          remoteChunkExecution.getSentCount(),
          remoteChunkExecution.getReceivedCount(),
          remoteChunkExecution.getExitStatus().getExitCode(),
          truncateExitDescription(remoteChunkExecution.getExitStatus().getExitDescription()),
          remoteChunkExecution.getLastUpdated()
        };
    int[] parameterTypes =
        new int[] {
          Types.BIGINT,
          Types.BIGINT,
          Types.TIMESTAMP,
          Types.TIMESTAMP,
          Types.VARCHAR,
          Types.INTEGER,
          Types.INTEGER,
          Types.INTEGER,
          Types.INTEGER,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.TIMESTAMP
        };

    getJdbcTemplate()
        .update(
            buildSql(tableSuffix, SAVE_REMOTE_CHUNK_EXECUTION), parameterValues, parameterTypes);
  }

  @Override
  public void updateRemoteChunkExecution(
      String tableSuffix, RemoteChunkExecution remoteChunkExecution) {
    Object[] parameterValues =
        new Object[] {
          remoteChunkExecution.getStartTime(),
          remoteChunkExecution.getEndTime(),
          remoteChunkExecution.getStatus(),
          remoteChunkExecution.getItemCount(),
          remoteChunkExecution.getCompletedCount(),
          remoteChunkExecution.getSentCount(),
          remoteChunkExecution.getReceivedCount(),
          remoteChunkExecution.getExitStatus().getExitCode(),
          truncateExitDescription(remoteChunkExecution.getExitStatus().getExitDescription()),
          remoteChunkExecution.getLastUpdated(),
          remoteChunkExecution.getStepExecutionId(),
          remoteChunkExecution.getSequence()
        };
    int[] parameterTypes =
        new int[] {
          Types.TIMESTAMP,
          Types.TIMESTAMP,
          Types.VARCHAR,
          Types.INTEGER,
          Types.INTEGER,
          Types.INTEGER,
          Types.INTEGER,
          Types.VARCHAR,
          Types.VARCHAR,
          Types.TIMESTAMP,
          Types.BIGINT,
          Types.BIGINT
        };

    getJdbcTemplate()
        .update(
            buildSql(tableSuffix, UPDATE_REMOTE_CHUNK_EXECUTION), parameterValues, parameterTypes);
  }

  private String truncateExitDescription(String description) {
    if (description != null && description.length() > exitMessageLength) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Truncating long message before update of RemoteChunkExecution, original message is: "
                + description);
      }
      return description.substring(0, exitMessageLength);
    } else {
      return description;
    }
  }

  @Override
  public RemoteChunkExecution getRemoteChunkExecution(
      String tableSuffix, Long stepExecutionId, Long sequence) {
    List<RemoteChunkExecution> remoteChunkExecutions =
        getJdbcTemplate()
            .query(
                buildSql(tableSuffix, GET_REMOTE_CHUNK_EXECUTION),
                new RemoteChunkExecutionRowMapper(),
                stepExecutionId,
                sequence);

    return remoteChunkExecutions.isEmpty() ? null : remoteChunkExecutions.get(0);
  }

  private String buildSql(String tableSuffix, String sqlString) {
    return getQuery(sqlString.replaceFirst("%SUFFIX%", tableSuffix));
  }

  private static class RemoteChunkExecutionRowMapper implements RowMapper<RemoteChunkExecution> {

    @Override
    public RemoteChunkExecution mapRow(ResultSet rs, int rowNum) throws SQLException {
      RemoteChunkExecution remoteChunkExecution =
          new RemoteChunkExecution(rs.getLong("STEP_EXECUTION_ID"), rs.getLong("SEQUENCE"));

      remoteChunkExecution.setStartTime(rs.getDate("START_TIME"));
      remoteChunkExecution.setEndTime(rs.getDate("END_TIME"));
      remoteChunkExecution.setStatus(BatchStatus.valueOf(rs.getString("STATUS")));

      remoteChunkExecution.setItemCount(rs.getInt("ITEM_COUNT"));
      remoteChunkExecution.setCompletedCount(rs.getInt("COMPLETED_COUNT"));
      remoteChunkExecution.setSentCount(rs.getInt("SENT_COUNT"));
      remoteChunkExecution.setReceivedCount(rs.getInt("RECEIVED_COUNT"));

      remoteChunkExecution.setExitStatus(
          new ExitStatus(rs.getString("EXIT_CODE"), rs.getString("EXIT_MESSAGE")));
      remoteChunkExecution.setLastUpdated(rs.getDate("LAST_UPDATED"));

      return remoteChunkExecution;
    }
  }

  @Override
  public Long countRemoteChunkExecutionByStatus(
      String tableSuffix, Long stepExecutionId, BatchStatus status) {
    return getJdbcTemplate()
        .queryForObject(
            buildSql(tableSuffix, COUNT_REMOTE_CHUNK_EXECUTION_BY_STATUS),
            Long.class,
            stepExecutionId,
            status.name());
  }
}
