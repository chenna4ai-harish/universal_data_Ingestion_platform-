import { F as FlowGraphBlock, f as defaultValueSerializationFunction } from './KHR_interactivity-CFFtxmyx.js';
import { j as getRichTypeFromValue } from './declarationMapper--xBLsv0n.js';
import { R as RegisterClass } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';
import './objectModelMapping-CcqDI7GW.js';

/**
 * Block that returns a constant value.
 */
class FlowGraphConstantBlock extends FlowGraphBlock {
    constructor(
    /**
     * the configuration of the block
     */
    config) {
        super(config);
        this.config = config;
        this.output = this.registerDataOutput("output", getRichTypeFromValue(config.value));
    }
    _updateOutputs(context) {
        this.output.setValue(this.config.value, context);
    }
    /**
     * Gets the class name of this block
     * @returns the class name
     */
    getClassName() {
        return "FlowGraphConstantBlock" /* FlowGraphBlockNames.Constant */;
    }
    /**
     * Serializes this block
     * @param serializationObject the object to serialize to
     * @param valueSerializeFunction the function to use to serialize the value
     */
    serialize(serializationObject = {}, valueSerializeFunction = defaultValueSerializationFunction) {
        super.serialize(serializationObject);
        valueSerializeFunction("value", this.config.value, serializationObject.config);
    }
}
RegisterClass("FlowGraphConstantBlock" /* FlowGraphBlockNames.Constant */, FlowGraphConstantBlock);

export { FlowGraphConstantBlock };
//# sourceMappingURL=flowGraphConstantBlock-B12yQrKj.js.map
