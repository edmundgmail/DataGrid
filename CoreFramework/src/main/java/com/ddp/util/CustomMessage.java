package com.ddp.util;

/**
 * Custom message for example
 * @author Junbong
 */
public class CustomMessage {
  public static final int STATUS_SUCCESS = 200;

  public static final int STATUS_GET_JOBS = 1;
  public static final int STATUS_POST_JOBS = 2;

  private final int statusCode;
  private final String resultCode;
  private final String summary;

  public CustomMessage(int statusCode, String resultCode, String summary) {
    this.statusCode = statusCode;
    this.resultCode = resultCode;
    this.summary = summary;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("CustomMessage{");
    sb.append("statusCode=").append(statusCode);
    sb.append(", resultCode='").append(resultCode).append('\'');
    sb.append(", summary='").append(summary).append('\'');
    sb.append('}');
    return sb.toString();
  }

  public int getStatusCode() {
    return statusCode;
  }

  public String getResultCode() {
    return resultCode;
  }

  public String getSummary() {
    return summary;
  }
}
