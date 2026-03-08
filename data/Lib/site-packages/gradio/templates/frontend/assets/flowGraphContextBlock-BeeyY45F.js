import { F as FlowGraphBlock } from './KHR_interactivity-CFFtxmyx.js';
import { R as RichTypeAny, b as RichTypeNumber } from './declarationMapper--xBLsv0n.js';
import { R as RegisterClass } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';
import './objectModelMapping-CcqDI7GW.js';

/**
 * A block that outputs elements from the context
 */
class FlowGraphContextBlock extends FlowGraphBlock {
    constructor(config) {
        super(config);
        this.userVariables = this.registerDataOutput("userVariables", RichTypeAny);
        this.executionId = this.registerDataOutput("executionId", RichTypeNumber);
    }
    _updateOutputs(context) {
        this.userVariables.setValue(context.userVariables, context);
        this.executionId.setValue(context.executionId, context);
    }
    serialize(serializationObject) {
        super.serialize(serializationObject);
    }
    getClassName() {
        return "FlowGraphContextBlock" /* FlowGraphBlockNames.Context */;
    }
}
RegisterClass("FlowGraphContextBlock" /* FlowGraphBlockNames.Context */, FlowGraphContextBlock);

export { FlowGraphContextBlock };
//# sourceMappingURL=flowGraphContextBlock-BeeyY45F.js.map
