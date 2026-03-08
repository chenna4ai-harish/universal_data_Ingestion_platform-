import { F as FlowGraphBlock } from './KHR_interactivity-CFFtxmyx.js';
import { c as RichTypeBoolean, R as RichTypeAny } from './declarationMapper--xBLsv0n.js';
import { R as RegisterClass } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';
import './objectModelMapping-CcqDI7GW.js';

/**
 * Block that returns a value based on a condition.
 */
class FlowGraphConditionalDataBlock extends FlowGraphBlock {
    /**
     * Creates a new instance of the block
     * @param config optional configuration for this block
     */
    constructor(config) {
        super(config);
        this.condition = this.registerDataInput("condition", RichTypeBoolean);
        this.onTrue = this.registerDataInput("onTrue", RichTypeAny);
        this.onFalse = this.registerDataInput("onFalse", RichTypeAny);
        this.output = this.registerDataOutput("output", RichTypeAny);
    }
    /**
     * @internal
     */
    _updateOutputs(context) {
        // get the value of the condition
        const condition = this.condition.getValue(context);
        // set the value based on the condition truth-ness.
        this.output.setValue(condition ? this.onTrue.getValue(context) : this.onFalse.getValue(context), context);
    }
    /**
     * Gets the class name of this block
     * @returns the class name
     */
    getClassName() {
        return "FlowGraphConditionalBlock" /* FlowGraphBlockNames.Conditional */;
    }
}
RegisterClass("FlowGraphConditionalBlock" /* FlowGraphBlockNames.Conditional */, FlowGraphConditionalDataBlock);

export { FlowGraphConditionalDataBlock };
//# sourceMappingURL=flowGraphConditionalDataBlock-DwoldQJJ.js.map
