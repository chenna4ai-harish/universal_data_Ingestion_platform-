import { S as ShaderStore } from './index-BeVTUo08.js';
import './helperFunctions-tvUh39Xc.js';
import './index-Bq2njFOY.js';

// Do not edit.
const name = "rgbdEncodePixelShader";
const shader = `varying vUV: vec2f;var textureSamplerSampler: sampler;var textureSampler: texture_2d<f32>;
#include<helperFunctions>
#define CUSTOM_FRAGMENT_DEFINITIONS
@fragment
fn main(input: FragmentInputs)->FragmentOutputs {fragmentOutputs.color=toRGBD(textureSample(textureSampler,textureSamplerSampler,input.vUV).rgb);}`;
// Sideeffect
if (!ShaderStore.ShadersStoreWGSL[name]) {
    ShaderStore.ShadersStoreWGSL[name] = shader;
}
/** @internal */
const rgbdEncodePixelShaderWGSL = { name, shader };

export { rgbdEncodePixelShaderWGSL };
//# sourceMappingURL=rgbdEncode.fragment-D5NsASzJ.js.map
