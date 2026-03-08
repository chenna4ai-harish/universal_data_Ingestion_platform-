import { S as ShaderStore } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';

// Do not edit.
const name = "passPixelShader";
const shader = `varying vec2 vUV;uniform sampler2D textureSampler;
#define CUSTOM_FRAGMENT_DEFINITIONS
void main(void) 
{gl_FragColor=texture2D(textureSampler,vUV);}`;
// Sideeffect
if (!ShaderStore.ShadersStore[name]) {
    ShaderStore.ShadersStore[name] = shader;
}
/** @internal */
const passPixelShader = { name, shader };

export { passPixelShader };
//# sourceMappingURL=pass.fragment-UmgPaaDj.js.map
