/*
 * Copyright 2024 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.runtime;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
@AllArgsConstructor(staticName = "of")
@ToString
public class AllocationConstraints {
    MachineDefinition machineDefinition;

    Map<String, String> assignmentAttributes;

    public double fitness(AllocationConstraints constraints, Map<String, String> allocationConstraintsAndDefaults) {
        if (!areAllocationConstraintsSatisfied(constraints, allocationConstraintsAndDefaults)) {
            return 0.0;
        }
        return machineDefinition.fitness(constraints.getMachineDefinition());
    }

    private boolean areAllocationConstraintsSatisfied(AllocationConstraints constraints, Map<String, String> allocationConstraintsAndDefaults) {
        return allocationConstraintsAndDefaults.entrySet()
            .stream()
            .allMatch(entry -> this.getAssignmentAttributes()
                .getOrDefault(entry.getKey(), entry.getValue())
                .equalsIgnoreCase(constraints.getAssignmentAttributes()
                    .getOrDefault(entry.getKey(), entry.getValue())));
    }
}
