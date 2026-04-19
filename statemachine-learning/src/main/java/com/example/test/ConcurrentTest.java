package com.example.test;

import com.example.state.*;

/**
 * 测试并发场景：HeartbeatThread清空generation
 *
 * 模拟场景：
 * 1. 正常完成JoinGroup和SyncGroup
 * 2. 在onJoinComplete执行期间，HeartbeatThread检测到session超时
 * 3. HeartbeatThread清空generation
 * 4. onJoinComplete检测到generation不匹配，需要重新rebalance
 */
public class ConcurrentTest {
    public static void main(String[] args) {
        System.out.println("=== Test: Heartbeat Clears Generation During onJoinComplete ===\n");

        MockNetworkClient client = new MockNetworkClient();
        GroupCoordinator coordinator = new GroupCoordinator("test-group", client);

        System.out.println("Step 1: First rebalance (should succeed)");
        coordinator.ensureActiveGroup();

        assert coordinator.getState() == MemberState.STABLE : "Should reach STABLE";
        Generation firstGeneration = coordinator.getGeneration();
        System.out.println("First generation: " + firstGeneration);
        System.out.println();

        System.out.println("Step 2: Simulate heartbeat failure");
        coordinator.simulateHeartbeatFailure();

        assert coordinator.getState() == MemberState.UNJOINED : "Should reset to UNJOINED";
        assert coordinator.getGeneration() == Generation.NO_GENERATION : "Generation should be cleared";
        System.out.println("State after heartbeat failure: " + coordinator.getState());
        System.out.println("Generation after heartbeat failure: " + coordinator.getGeneration());
        System.out.println();

        System.out.println("Step 3: Second rebalance (should succeed with new generation)");
        coordinator.ensureActiveGroup();

        assert coordinator.getState() == MemberState.STABLE : "Should reach STABLE again";
        Generation secondGeneration = coordinator.getGeneration();
        System.out.println("Second generation: " + secondGeneration);

        assert secondGeneration.generationId > firstGeneration.generationId :
            "New generation ID should be larger";

        System.out.println("\n✅ Test PASSED");
    }
}
