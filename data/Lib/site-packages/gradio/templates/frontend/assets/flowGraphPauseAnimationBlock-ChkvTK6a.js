import { b as FlowGraphExecutionBlockWithOutSignal } from './KHR_interactivity-CFFtxmyx.js';
import { R as RichTypeAny } from './declarationMapper--xBLsv0n.js';
import { R as RegisterClass } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';
import './objectModelMapping-CcqDI7GW.js';

/**
 * @experimental
 * Block that pauses a running animation
 */
class FlowGraphPauseAnimationBlock extends FlowGraphExecutionBlockWithOutSignal {
    constructor(config) {
        super(config);
        this.animationToPause = this.registerDataInput("animationToPause", RichTypeAny);
    }
    _execute(context) {
        const animationToPauseValue = this.animationToPause.getValue(context);
        animationToPauseValue.pause();
        this.out._activateSignal(context);
    }
    /**
     * @returns class name of the block.
     */
    getClassName() {
        return "FlowGraphPauseAnimationBlock" /* FlowGraphBlockNames.PauseAnimation */;
    }
}
RegisterClass("FlowGraphPauseAnimationBlock" /* FlowGraphBlockNames.PauseAnimation */, FlowGraphPauseAnimationBlock);

export { FlowGraphPauseAnimationBlock };
//# sourceMappingURL=flowGraphPauseAnimationBlock-ChkvTK6a.js.map
