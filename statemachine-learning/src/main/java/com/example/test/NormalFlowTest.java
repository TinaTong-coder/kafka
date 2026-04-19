package com.example.test;

import com.example.state.*;

/**
 * 测试正常的Rebalance流程
 *
 * 验证：UNJOINED → PREPARING_REBALANCE → COMPLETING_REBALANCE → STABLE
 */
public class NormalFlowTest {
    public static void main(String[] args) {
        System.out.println("=== Test: Normal Rebalance Flow ===\n");

        MockNetworkClient client = new MockNetworkClient();
        GroupCoordinator coordinator = new GroupCoordinator("test-group", client);

        // 初始状态检查
        assert coordinator.getState() == MemberState.UNJOINED : "Initial state should be UNJOINED";
        assert coordinator.getGeneration() == Generation.NO_GENERATION : "Initial generation should be NO_GENERATION";

        System.out.println("Initial state: " + coordinator.getState());
        System.out.println("Initial generation: " + coordinator.getGeneration());
        System.out.println();

        // 执行rebalance
        coordinator.ensureActiveGroup();

        // 验证最终状态
        System.out.println();
        System.out.println("=== Final State ===");
        System.out.println("State: " + coordinator.getState());
        System.out.println("Generation: " + coordinator.getGeneration());

        assert coordinator.getState() == MemberState.STABLE : "Final state should be STABLE";
        assert coordinator.getGeneration() != Generation.NO_GENERATION : "Should have valid generation";
        assert coordinator.getGeneration().generationId > 0 : "Generation ID should be positive";

        System.out.println("\n✅ Test PASSED");
    }
}
