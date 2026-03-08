import { a as SceneComponentConstants, b as Scene } from './index-BeVTUo08.js';
import { G as GeometryBufferRenderer } from './geometryBufferRenderer-BZRBLo1Z.js';
import './index-Bq2njFOY.js';
import './clipPlaneFragment-BnjdBepB.js';
import './bumpFragment-OwQ9_gWh.js';
import './helperFunctions-KDBcQUkc.js';
import './bakedVertexAnimation-BPhDzI-J.js';
import './morphTargetsVertex-Cb-ex_-Y.js';
import './instancesDeclaration-56SqPgF6.js';
import './sceneUboDeclaration-xBDMcG2T.js';
import './clipPlaneVertex-M-hEKWgW.js';
import './bumpVertex-Biw93-H4.js';

Object.defineProperty(Scene.prototype, "geometryBufferRenderer", {
    get: function () {
        return this._geometryBufferRenderer;
    },
    set: function (value) {
        if (value && value.isSupported) {
            this._geometryBufferRenderer = value;
        }
    },
    enumerable: true,
    configurable: true,
});
Scene.prototype.enableGeometryBufferRenderer = function (ratio = 1, depthFormat = 15, textureTypesAndFormats) {
    if (this._geometryBufferRenderer) {
        return this._geometryBufferRenderer;
    }
    this._geometryBufferRenderer = new GeometryBufferRenderer(this, ratio, depthFormat, textureTypesAndFormats);
    if (!this._geometryBufferRenderer.isSupported) {
        this._geometryBufferRenderer = null;
    }
    return this._geometryBufferRenderer;
};
Scene.prototype.disableGeometryBufferRenderer = function () {
    if (!this._geometryBufferRenderer) {
        return;
    }
    this._geometryBufferRenderer.dispose();
    this._geometryBufferRenderer = null;
};
/**
 * Defines the Geometry Buffer scene component responsible to manage a G-Buffer useful
 * in several rendering techniques.
 */
class GeometryBufferRendererSceneComponent {
    /**
     * Creates a new instance of the component for the given scene
     * @param scene Defines the scene to register the component in
     */
    constructor(scene) {
        /**
         * The component name helpful to identify the component in the list of scene components.
         */
        this.name = SceneComponentConstants.NAME_GEOMETRYBUFFERRENDERER;
        this.scene = scene;
    }
    /**
     * Registers the component in a given scene
     */
    register() {
        this.scene._gatherRenderTargetsStage.registerStep(SceneComponentConstants.STEP_GATHERRENDERTARGETS_GEOMETRYBUFFERRENDERER, this, this._gatherRenderTargets);
    }
    /**
     * Rebuilds the elements related to this component in case of
     * context lost for instance.
     */
    rebuild() {
        // Nothing to do for this component
    }
    /**
     * Disposes the component and the associated resources
     */
    dispose() {
        // Nothing to do for this component
    }
    _gatherRenderTargets(renderTargets) {
        if (this.scene._geometryBufferRenderer) {
            renderTargets.push(this.scene._geometryBufferRenderer.getGBuffer());
        }
    }
}
GeometryBufferRenderer._SceneComponentInitialization = (scene) => {
    // Register the G Buffer component to the scene.
    let component = scene._getComponent(SceneComponentConstants.NAME_GEOMETRYBUFFERRENDERER);
    if (!component) {
        component = new GeometryBufferRendererSceneComponent(scene);
        scene._addComponent(component);
    }
};

export { GeometryBufferRendererSceneComponent };
//# sourceMappingURL=geometryBufferRendererSceneComponent-DvgK0R-d.js.map
