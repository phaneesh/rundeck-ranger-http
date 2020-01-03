package com.raven.rundeck.ranger.http;

import lombok.*;

/**
 * Basic shard info used for discovery.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
@ToString
public class ShardInfo {
  private String environment;
}