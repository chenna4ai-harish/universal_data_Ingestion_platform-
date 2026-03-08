import { GLTFLoader, ArrayItem } from './glTFLoader-CnPvpk_i.js';
import { aG as unregisterGLTFExtension, aH as registerGLTFExtension } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';
import './bone-DUz1PWqo.js';
import './skeleton-CW7HrChv.js';
import './rawTexture-BXEcRnE4.js';
import './assetContainer-C-rbJgui.js';
import './objectModelMapping-CcqDI7GW.js';

const NAME = "KHR_texture_basisu";
/**
 * [Specification](https://github.com/KhronosGroup/glTF/blob/main/extensions/2.0/Khronos/KHR_texture_basisu/README.md)
 */
// eslint-disable-next-line @typescript-eslint/naming-convention
class KHR_texture_basisu {
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
            }, texture._textureInfo.nonColorData ? { useRGBAIfASTCBC7NotAvailableWhenUASTC: true } : undefined, !texture._textureInfo.nonColorData);
        });
    }
}
unregisterGLTFExtension(NAME);
registerGLTFExtension(NAME, true, (loader) => new KHR_texture_basisu(loader));

export { KHR_texture_basisu };
//# sourceMappingURL=KHR_texture_basisu-C19-qo2q.js.map
