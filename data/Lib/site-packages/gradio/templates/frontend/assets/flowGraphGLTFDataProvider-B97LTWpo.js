import { F as FlowGraphBlock } from './KHR_interactivity-CFFtxmyx.js';
import { R as RichTypeAny } from './declarationMapper--xBLsv0n.js';
import './index-Bq2njFOY.js';
import './index-BeVTUo08.js';
import './objectModelMapping-CcqDI7GW.js';

/**
 * a glTF-based FlowGraph block that provides arrays with babylon object, based on the glTF tree
 * Can be used, for example, to get animation index from a glTF animation
 */
class FlowGraphGLTFDataProvider extends FlowGraphBlock {
    constructor(config) {
        super();
        const glTF = config.glTF;
        const animationGroups = glTF.animations?.map((a) => a._babylonAnimationGroup) || [];
        this.animationGroups = this.registerDataOutput("animationGroups", RichTypeAny, animationGroups);
        const nodes = glTF.nodes?.map((n) => n._babylonTransformNode) || [];
        this.nodes = this.registerDataOutput("nodes", RichTypeAny, nodes);
    }
    getClassName() {
        return "FlowGraphGLTFDataProvider";
    }
}

export { FlowGraphGLTFDataProvider };
//# sourceMappingURL=flowGraphGLTFDataProvider-B97LTWpo.js.map
