import { F as FlowGraphUnaryOperationBlock } from './flowGraphUnaryOperationBlock-CNA1lVJO.js';
import { b as RichTypeNumber, i as RichTypeFlowGraphInteger, F as FlowGraphInteger, c as RichTypeBoolean } from './declarationMapper--xBLsv0n.js';
import { R as RegisterClass } from './index-BeVTUo08.js';
import './flowGraphCachedOperationBlock--1LW-bWu.js';
import './KHR_interactivity-CFFtxmyx.js';
import './index-Bq2njFOY.js';
import './objectModelMapping-CcqDI7GW.js';

/**
 * A block that converts a boolean to a float.
 */
class FlowGraphBooleanToFloat extends FlowGraphUnaryOperationBlock {
    constructor(config) {
        super(RichTypeBoolean, RichTypeNumber, (a) => +a, "FlowGraphBooleanToFloat" /* FlowGraphBlockNames.BooleanToFloat */, config);
    }
}
RegisterClass("FlowGraphBooleanToFloat" /* FlowGraphBlockNames.BooleanToFloat */, FlowGraphBooleanToFloat);
/**
 * A block that converts a boolean to an integer
 */
class FlowGraphBooleanToInt extends FlowGraphUnaryOperationBlock {
    constructor(config) {
        super(RichTypeBoolean, RichTypeFlowGraphInteger, (a) => FlowGraphInteger.FromValue(+a), "FlowGraphBooleanToInt" /* FlowGraphBlockNames.BooleanToInt */, config);
    }
}
RegisterClass("FlowGraphBooleanToInt" /* FlowGraphBlockNames.BooleanToInt */, FlowGraphBooleanToInt);
/**
 * A block that converts a float to a boolean.
 */
class FlowGraphFloatToBoolean extends FlowGraphUnaryOperationBlock {
    constructor(config) {
        super(RichTypeNumber, RichTypeBoolean, (a) => !!a, "FlowGraphFloatToBoolean" /* FlowGraphBlockNames.FloatToBoolean */, config);
    }
}
RegisterClass("FlowGraphFloatToBoolean" /* FlowGraphBlockNames.FloatToBoolean */, FlowGraphFloatToBoolean);
/**
 * A block that converts an integer to a boolean.
 */
class FlowGraphIntToBoolean extends FlowGraphUnaryOperationBlock {
    constructor(config) {
        super(RichTypeFlowGraphInteger, RichTypeBoolean, (a) => !!a.value, "FlowGraphIntToBoolean" /* FlowGraphBlockNames.IntToBoolean */, config);
    }
}
RegisterClass("FlowGraphIntToBoolean" /* FlowGraphBlockNames.IntToBoolean */, FlowGraphIntToBoolean);
/**
 * A block that converts an integer to a float.
 */
class FlowGraphIntToFloat extends FlowGraphUnaryOperationBlock {
    constructor(config) {
        super(RichTypeFlowGraphInteger, RichTypeNumber, (a) => a.value, "FlowGraphIntToFloat" /* FlowGraphBlockNames.IntToFloat */, config);
    }
}
RegisterClass("FlowGraphIntToFloat" /* FlowGraphBlockNames.IntToFloat */, FlowGraphIntToFloat);
/**
 * A block that converts a float to an integer.
 */
class FlowGraphFloatToInt extends FlowGraphUnaryOperationBlock {
    constructor(config) {
        super(RichTypeNumber, RichTypeFlowGraphInteger, (a) => {
            const roundingMode = config?.roundingMode;
            switch (roundingMode) {
                case "floor":
                    return FlowGraphInteger.FromValue(Math.floor(a));
                case "ceil":
                    return FlowGraphInteger.FromValue(Math.ceil(a));
                case "round":
                    return FlowGraphInteger.FromValue(Math.round(a));
                default:
                    return FlowGraphInteger.FromValue(a);
            }
        }, "FlowGraphFloatToInt" /* FlowGraphBlockNames.FloatToInt */, config);
    }
}
RegisterClass("FlowGraphFloatToInt" /* FlowGraphBlockNames.FloatToInt */, FlowGraphFloatToInt);

export { FlowGraphBooleanToFloat, FlowGraphBooleanToInt, FlowGraphFloatToBoolean, FlowGraphFloatToInt, FlowGraphIntToBoolean, FlowGraphIntToFloat };
//# sourceMappingURL=flowGraphTypeToTypeBlocks-DrAYxUeR.js.map
