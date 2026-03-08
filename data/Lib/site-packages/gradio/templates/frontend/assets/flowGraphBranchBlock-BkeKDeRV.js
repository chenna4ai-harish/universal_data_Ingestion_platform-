import { c as RichTypeBoolean } from './declarationMapper--xBLsv0n.js';
import { e as FlowGraphExecutionBlock } from './KHR_interactivity-CFFtxmyx.js';
import { R as RegisterClass } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';
import './objectModelMapping-CcqDI7GW.js';

/**
 * A block that evaluates a condition and activates one of two branches.
 */
class FlowGraphBranchBlock extends FlowGraphExecutionBlock {
    constructor(config) {
        super(config);
        this.condition = this.registerDataInput("condition", RichTypeBoolean);
        this.onTrue = this._registerSignalOutput("onTrue");
        this.onFalse = this._registerSignalOutput("onFalse");
    }
    _execute(context) {
        if (this.condition.getValue(context)) {
            this.onTrue._activateSignal(context);
        }
        else {
            this.onFalse._activateSignal(context);
        }
    }
    /**
     * @returns class name of the block.
     */
    getClassName() {
        return "FlowGraphBranchBlock" /* FlowGraphBlockNames.Branch */;
    }
}
RegisterClass("FlowGraphBranchBlock" /* FlowGraphBlockNames.Branch */, FlowGraphBranchBlock);

export { FlowGraphBranchBlock };
//# sourceMappingURL=flowGraphBranchBlock-BkeKDeRV.js.map
