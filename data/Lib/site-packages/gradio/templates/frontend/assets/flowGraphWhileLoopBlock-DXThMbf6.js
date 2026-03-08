import { c as RichTypeBoolean } from './declarationMapper--xBLsv0n.js';
import { L as Logger, R as RegisterClass } from './index-BeVTUo08.js';
import { b as FlowGraphExecutionBlockWithOutSignal } from './KHR_interactivity-CFFtxmyx.js';
import './index-Bq2njFOY.js';
import './objectModelMapping-CcqDI7GW.js';

/**
 * A block that executes a branch while a condition is true.
 */
class FlowGraphWhileLoopBlock extends FlowGraphExecutionBlockWithOutSignal {
    constructor(
    /**
     * the configuration of the block
     */
    config) {
        super(config);
        this.config = config;
        this.condition = this.registerDataInput("condition", RichTypeBoolean);
        this.executionFlow = this._registerSignalOutput("executionFlow");
        this.completed = this._registerSignalOutput("completed");
        // unregister "out" signal
        this._unregisterSignalOutput("out");
    }
    _execute(context, _callingSignal) {
        let conditionValue = this.condition.getValue(context);
        if (this.config?.doWhile && !conditionValue) {
            this.executionFlow._activateSignal(context);
        }
        let i = 0;
        while (conditionValue) {
            this.executionFlow._activateSignal(context);
            ++i;
            if (i >= FlowGraphWhileLoopBlock.MaxLoopCount) {
                Logger.Warn("FlowGraphWhileLoopBlock: Max loop count reached. Breaking.");
                break;
            }
            conditionValue = this.condition.getValue(context);
        }
        // out is not triggered - completed is triggered
        this.completed._activateSignal(context);
    }
    getClassName() {
        return "FlowGraphWhileLoopBlock" /* FlowGraphBlockNames.WhileLoop */;
    }
}
/**
 * The maximum number of iterations allowed in a loop.
 * This can be set to avoid an infinite loop.
 */
FlowGraphWhileLoopBlock.MaxLoopCount = 1000;
RegisterClass("FlowGraphWhileLoopBlock" /* FlowGraphBlockNames.WhileLoop */, FlowGraphWhileLoopBlock);

export { FlowGraphWhileLoopBlock };
//# sourceMappingURL=flowGraphWhileLoopBlock-DXThMbf6.js.map
