import { c as FlowGraphEventBlock } from './KHR_interactivity-CFFtxmyx.js';
import { R as RegisterClass } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';
import './declarationMapper--xBLsv0n.js';
import './objectModelMapping-CcqDI7GW.js';

/**
 * Block that triggers when a scene is ready.
 */
class FlowGraphSceneReadyEventBlock extends FlowGraphEventBlock {
    constructor() {
        super(...arguments);
        this.initPriority = -1;
        this.type = "SceneReady" /* FlowGraphEventType.SceneReady */;
    }
    _executeEvent(context, _payload) {
        this._execute(context);
        return true;
    }
    _preparePendingTasks(context) {
        // no-op
    }
    _cancelPendingTasks(context) {
        // no-op
    }
    /**
     * @returns class name of the block.
     */
    getClassName() {
        return "FlowGraphSceneReadyEventBlock" /* FlowGraphBlockNames.SceneReadyEvent */;
    }
}
RegisterClass("FlowGraphSceneReadyEventBlock" /* FlowGraphBlockNames.SceneReadyEvent */, FlowGraphSceneReadyEventBlock);

export { FlowGraphSceneReadyEventBlock };
//# sourceMappingURL=flowGraphSceneReadyEventBlock-BK98rPFB.js.map
