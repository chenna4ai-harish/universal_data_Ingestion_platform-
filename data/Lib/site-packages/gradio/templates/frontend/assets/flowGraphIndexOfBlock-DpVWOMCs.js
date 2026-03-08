import { F as FlowGraphBlock } from './KHR_interactivity-CFFtxmyx.js';
import { R as RichTypeAny, i as RichTypeFlowGraphInteger, F as FlowGraphInteger } from './declarationMapper--xBLsv0n.js';
import { R as RegisterClass } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';
import './objectModelMapping-CcqDI7GW.js';

/**
 * This block takes an object as input and an array and returns the index of the object in the array.
 */
class FlowGraphIndexOfBlock extends FlowGraphBlock {
    /**
     * Construct a FlowGraphIndexOfBlock.
     * @param config construction parameters
     */
    constructor(config) {
        super(config);
        this.config = config;
        this.object = this.registerDataInput("object", RichTypeAny);
        this.array = this.registerDataInput("array", RichTypeAny);
        this.index = this.registerDataOutput("index", RichTypeFlowGraphInteger, new FlowGraphInteger(-1));
    }
    /**
     * @internal
     */
    _updateOutputs(context) {
        const object = this.object.getValue(context);
        const array = this.array.getValue(context);
        if (array) {
            this.index.setValue(new FlowGraphInteger(array.indexOf(object)), context);
        }
    }
    /**
     * Serializes this block
     * @param serializationObject the object to serialize to
     */
    serialize(serializationObject) {
        super.serialize(serializationObject);
    }
    getClassName() {
        return "FlowGraphIndexOfBlock" /* FlowGraphBlockNames.IndexOf */;
    }
}
RegisterClass("FlowGraphIndexOfBlock" /* FlowGraphBlockNames.IndexOf */, FlowGraphIndexOfBlock);

export { FlowGraphIndexOfBlock };
//# sourceMappingURL=flowGraphIndexOfBlock-DpVWOMCs.js.map
