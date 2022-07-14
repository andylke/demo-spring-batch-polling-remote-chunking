package com.github.andylke.demo.remotechunking;

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

public class JdbcChunkExecutionDao extends AbstractJdbcBatchMetadataDao
    implements ChunkExecutionDao, InitializingBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcChunkExecutionDao.class);

  private static final String CHUNK_EXECUTION_TABLE = "%PREFIX%CHUNK_EXECUTION_%SUFFIX%";

  private static final String SAVE_CHUNK_EXECUTION =
      "INSERT INTO "
          + CHUNK_EXECUTION_TABLE
          + " (STEP_EXECUTION_ID, SEQUENCE,"
          + " START_TIME, END_TIME, STATUS,"
          + " ITEM_COUNT, SENT_COUNT, RECEIVED_COUNT,"
          + " EXIT_CODE, EXIT_MESSAGE, LAST_UPDATED)"
          + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String UPDATE_CHUNK_EXECUTION =
      "UPDATE "
          + CHUNK_EXECUTION_TABLE
          + " SET"
          + " START_TIME = ?, END_TIME = ?, STATUS = ?,"
          + " ITEM_COUNT = ?, SENT_COUNT = ?, RECEIVED_COUNT = ?,"
          + " EXIT_CODE = ?, EXIT_MESSAGE = ?, LAST_UPDATED = ?"
          + " WHERE STEP_EXECUTION_ID = ? AND SEQUENCE = ?";

  private static final String GET_CHUNK_EXECUTION =
      "SELECT"
          + " STEP_EXECUTION_ID, SEQUENCE, START_TIME, END_TIME, STATUS,"
          + " ITEM_COUNT, SENT_COUNT, RECEIVED_COUNT,"
          + " EXIT_CODE, EXIT_MESSAGE, LAST_UPDATED"
          + " FROM "
          + CHUNK_EXECUTION_TABLE
          + " WHERE STEP_EXECUTION_ID = ? AND SEQUENCE = ?";

  private static final String COUNT_CHUNK_EXECUTION_BY_STATUS_NOT =
      "SELECT COUNT(*) FROM "
          + CHUNK_EXECUTION_TABLE
          + " WHERE STEP_EXECUTION_ID = ? AND STATUS <> ?";

  private static final String GET_CHUNK_EXECUTIONS_BY_STATUS_NOT =
      "SELECT"
          + " STEP_EXECUTION_ID, SEQUENCE, START_TIME, END_TIME, STATUS,"
          + " ITEM_COUNT, SENT_COUNT, RECEIVED_COUNT,"
          + " EXIT_CODE, EXIT_MESSAGE, LAST_UPDATED"
          + " FROM "
          + CHUNK_EXECUTION_TABLE
          + " WHERE STEP_EXECUTION_ID = ? AND STATUS <> ?";

  private int exitMessageLength = DEFAULT_EXIT_MESSAGE_LENGTH;

  public JdbcChunkExecutionDao(JdbcOperations jdbcTemplate) {
    setJdbcTemplate(jdbcTemplate);
  }

  @Override
  public void saveChunkExecution(String chunkTableSuffix, ChunkExecution chunkExecution) {
    Object[] parameterValues =
        new Object[] {
          chunkExecution.getStepExecutionId(),
          chunkExecution.getSequence(),
          chunkExecution.getStartTime(),
          chunkExecution.getEndTime(),
          chunkExecution.getStatus(),
          chunkExecution.getItemCount(),
          chunkExecution.getSentCount(),
          chunkExecution.getReceivedCount(),
          chunkExecution.getExitStatus().getExitCode(),
          truncateExitDescription(chunkExecution.getExitStatus().getExitDescription()),
          chunkExecution.getLastUpdated()
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
          Types.VARCHAR,
          Types.VARCHAR,
          Types.TIMESTAMP
        };

    getJdbcTemplate()
        .update(buildSql(chunkTableSuffix, SAVE_CHUNK_EXECUTION), parameterValues, parameterTypes);
  }

  @Override
  public void updateChunkExecution(String chunkTableSuffix, ChunkExecution chunkExecution) {
    Object[] parameterValues =
        new Object[] {
          chunkExecution.getStartTime(),
          chunkExecution.getEndTime(),
          chunkExecution.getStatus(),
          chunkExecution.getItemCount(),
          chunkExecution.getSentCount(),
          chunkExecution.getReceivedCount(),
          chunkExecution.getExitStatus().getExitCode(),
          truncateExitDescription(chunkExecution.getExitStatus().getExitDescription()),
          chunkExecution.getLastUpdated(),
          chunkExecution.getStepExecutionId(),
          chunkExecution.getSequence()
        };
    int[] parameterTypes =
        new int[] {
          Types.TIMESTAMP,
          Types.TIMESTAMP,
          Types.VARCHAR,
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
            buildSql(chunkTableSuffix, UPDATE_CHUNK_EXECUTION), parameterValues, parameterTypes);
  }

  private String truncateExitDescription(String description) {
    if (description != null && description.length() > exitMessageLength) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Truncating long message before update of ChunkExecution, original message is: "
                + description);
      }
      return description.substring(0, exitMessageLength);
    } else {
      return description;
    }
  }

  @Override
  public ChunkExecution getChunkExecution(
      String chunkTableSuffix, Long stepExecutionId, Long sequence) {
    List<ChunkExecution> chunkExecutions =
        getJdbcTemplate()
            .query(
                buildSql(chunkTableSuffix, GET_CHUNK_EXECUTION),
                new ChunkExecutionRowMapper(),
                stepExecutionId,
                sequence);

    return chunkExecutions.isEmpty() ? null : chunkExecutions.get(0);
  }

  @Override
  public List<ChunkExecution> getChunkExecutionsByStatusNot(
      String chunkTableSuffix, Long stepExecutionId, BatchStatus status) {
    return getJdbcTemplate()
        .query(
            buildSql(chunkTableSuffix, GET_CHUNK_EXECUTIONS_BY_STATUS_NOT),
            new ChunkExecutionRowMapper(),
            stepExecutionId,
            status.name());
  }

  @Override
  public Long countChunkExecutionByStatusNot(
      String chunkTableSuffix, Long stepExecutionId, BatchStatus status) {
    return getJdbcTemplate()
        .queryForObject(
            buildSql(chunkTableSuffix, COUNT_CHUNK_EXECUTION_BY_STATUS_NOT),
            Long.class,
            stepExecutionId,
            status.name());
  }

  private String buildSql(String chunkTableSuffix, String sqlString) {
    return getQuery(sqlString.replaceFirst("%SUFFIX%", chunkTableSuffix));
  }

  private static class ChunkExecutionRowMapper implements RowMapper<ChunkExecution> {

    @Override
    public ChunkExecution mapRow(ResultSet rs, int rowNum) throws SQLException {
      ChunkExecution chunkExecution =
          new ChunkExecution(rs.getLong("STEP_EXECUTION_ID"), rs.getLong("SEQUENCE"));

      chunkExecution.setStartTime(rs.getDate("START_TIME"));
      chunkExecution.setEndTime(rs.getDate("END_TIME"));
      chunkExecution.setStatus(BatchStatus.valueOf(rs.getString("STATUS")));

      chunkExecution.setItemCount(rs.getInt("ITEM_COUNT"));
      chunkExecution.setSentCount(rs.getInt("SENT_COUNT"));
      chunkExecution.setReceivedCount(rs.getInt("RECEIVED_COUNT"));

      chunkExecution.setExitStatus(
          new ExitStatus(rs.getString("EXIT_CODE"), rs.getString("EXIT_MESSAGE")));
      chunkExecution.setLastUpdated(rs.getDate("LAST_UPDATED"));

      return chunkExecution;
    }
  }
}
