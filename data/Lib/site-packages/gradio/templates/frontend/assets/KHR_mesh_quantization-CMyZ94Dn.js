import { aG as unregisterGLTFExtension, aH as registerGLTFExtension } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';

const NAME = "KHR_mesh_quantization";
/**
 * [Specification](https://github.com/KhronosGroup/glTF/blob/main/extensions/2.0/Khronos/KHR_mesh_quantization/README.md)
 */
// eslint-disable-next-line @typescript-eslint/naming-convention
class KHR_mesh_quantization {
    /**
     * @internal
     */
    constructor(loader) {
        /**
         * The name of this extension.
         */
        this.name = NAME;
        this.enabled = loader.isExtensionUsed(NAME);
    }
    /** @internal */
    dispose() { }
}
unregisterGLTFExtension(NAME);
registerGLTFExtension(NAME, true, (loader) => new KHR_mesh_quantization(loader));

export { KHR_mesh_quantization };
//# sourceMappingURL=KHR_mesh_quantization-CMyZ94Dn.js.map
