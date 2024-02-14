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

package io.mantisrx.server.core.scheduler;

import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.shaded.com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The `SizeDefinition` class defines the scheduling constraint for worker container sizes.
 * This constraint can be expressed in two ways:
 *   1. A formal machine definition, which includes attributes such as cores, memory, disk, and network bandwidth (represented by `machineDefinition`).
 *   2. A size label, which logically binds a size definition to the task executors of a given resource cluster specification.
 */
@Getter
@EqualsAndHashCode
@ToString
// TODO: think about the name...
public class SizeDefinition {
    public static final String SIZE_LABEL = "sizeLabel";

    /**
     * A reference to the `MachineDefinition` class instance, representing the formal machine specification
     */
    @Nullable
    MachineDefinition machineDefinition;

    /**
     * A label representing the size definition, which is used to logically bind a task executor to a given resource cluster specification. Ie. small, large
     */
    @Nullable
    String label;

    Map<String, String> tags;

    public SizeDefinition(MachineDefinition machineDefinition, String label) {
        this.machineDefinition = machineDefinition;
        this.label = (label == null || label.trim().isEmpty()) ? null : label.trim();

        // Check if both machineDefinition and label are null
        if (this.machineDefinition == null && this.label == null) {
            throw new IllegalArgumentException("At least one among MachineDefinition and label must be provided");
        }

        this.tags = label != null ? ImmutableMap.of(SIZE_LABEL, label) : ImmutableMap.of(
            "cpuCores",
            String.valueOf(machineDefinition.getCpuCores()),
            "memoryMB",
            String.valueOf(machineDefinition.getMemoryMB()));
    }

    public static SizeDefinition of(MachineDefinition machineDefinition, String label) {
        return new SizeDefinition(machineDefinition, label);
    }

    public static SizeDefinition of(MachineDefinition machineDefinition) {
        return new SizeDefinition(machineDefinition, null);
    }
}
