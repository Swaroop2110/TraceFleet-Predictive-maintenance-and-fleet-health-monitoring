Feature: Incident Detection
  The streaming rules engine should classify scenario signals without relying
  on an externally supplied incident label.

  Scenario: Collision-like chain produces enough distinct signals
    Given a collision-like telemetry sequence
    When the detection window evaluates the sequence
    Then harsh braking and sudden stop signals are present

  Scenario Outline: Required scenario outcomes follow the rules
    Given the "<name>" scenario
    When the incident engine processes its events
    Then the incident outcome matches hidden ground truth
    Examples:
      | name                      |
      | collision_like            |
      | overspeed_chain           |
      | false_positive_hard_brake |
      | diagnostic_only           |
      | duplicate_storm           |
      | out_of_order              |
      | late_arrival              |
      | schema_fault              |
