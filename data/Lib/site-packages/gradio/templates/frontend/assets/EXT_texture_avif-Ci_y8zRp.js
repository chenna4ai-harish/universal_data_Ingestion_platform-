import { GLTFLoader, ArrayItem } from './glTFLoader-CnPvpk_i.js';
import { aG as unregisterGLTFExtension, aH as registerGLTFExtension } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';
import './bone-DUz1PWqo.js';
import './skeleton-CW7HrChv.js';
import './rawTexture-BXEcRnE4.js';
import './assetContainer-C-rbJgui.js';
import './objectModelMapping-CcqDI7GW.js';

const NAME = "EXT_texture_avif";
/**
 * [glTF PR](https://github.com/KhronosGroup/glTF/pull/2235)
 * [Specification](https://github.com/KhronosGroup/glTF/blob/main/extensions/2.0/Vendor/EXT_texture_avif/README.md)
 */
// eslint-disable-next-line @typescript-eslint/naming-convention
class EXT_texture_avif {
    /**
     * @internal
     */
    constructor(loader) {
        /** The name of this extension. */
        this.name = NAME;
        this._loader = loader;
        this.enabled = loader.isExtensionUsed(NAME);
    }
    /** @internal */
    dispose() {
        this._loader = null;
    }
    /**
     * @internal
     */
    // eslint-disable-next-line no-restricted-syntax
    _loadTextureAsync(context, texture, assign) {
        return GLTFLoader.LoadExtensionAsync(context, texture, this.name, async (extensionContext, extension) => {
            const sampler = texture.sampler == undefined ? GLTFLoader.DefaultSampler : ArrayItem.Get(`${context}/sampler`, this._loader.gltf.samplers, texture.sampler);
            const image = ArrayItem.Get(`${extensionContext}/source`, this._loader.gltf.images, extension.source);
            return await this._loader._createTextureAsync(context, sampler, image, (babylonTexture) => {
                assign(babylonTexture);
            }, undefined, !texture._textureInfo.nonColorData);
        });
    }
}
unregisterGLTFExtension(NAME);
registerGLTFExtension(NAME, true, (loader) => new EXT_texture_avif(loader));

export { EXT_texture_avif };
//# sourceMappingURL=EXT_texture_avif-Ci_y8zRp.js.map
