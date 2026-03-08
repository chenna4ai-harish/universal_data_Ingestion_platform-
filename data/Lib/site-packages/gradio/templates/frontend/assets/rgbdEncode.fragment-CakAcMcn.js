import { S as ShaderStore } from './index-BeVTUo08.js';
import './helperFunctions-KDBcQUkc.js';
import './index-Bq2njFOY.js';

// Do not edit.
const name = "rgbdEncodePixelShader";
const shader = `varying vec2 vUV;uniform sampler2D textureSampler;
#include<helperFunctions>
#define CUSTOM_FRAGMENT_DEFINITIONS
void main(void) 
{gl_FragColor=toRGBD(texture2D(textureSampler,vUV).rgb);}`;
// Sideeffect
if (!ShaderStore.ShadersStore[name]) {
    ShaderStore.ShadersStore[name] = shader;
}
/** @internal */
const rgbdEncodePixelShader = { name, shader };

export { rgbdEncodePixelShader };
//# sourceMappingURL=rgbdEncode.fragment-CakAcMcn.js.map
