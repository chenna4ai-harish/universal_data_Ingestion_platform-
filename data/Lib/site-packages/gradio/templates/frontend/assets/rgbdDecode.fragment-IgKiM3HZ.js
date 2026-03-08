import { S as ShaderStore } from './index-BeVTUo08.js';
import './helperFunctions-tvUh39Xc.js';
import './index-Bq2njFOY.js';

// Do not edit.
const name = "rgbdDecodePixelShader";
const shader = `varying vUV: vec2f;var textureSamplerSampler: sampler;var textureSampler: texture_2d<f32>;
#include<helperFunctions>
#define CUSTOM_FRAGMENT_DEFINITIONS
@fragment
fn main(input: FragmentInputs)->FragmentOutputs {fragmentOutputs.color=vec4f(fromRGBD(textureSample(textureSampler,textureSamplerSampler,input.vUV)),1.0);}`;
// Sideeffect
if (!ShaderStore.ShadersStoreWGSL[name]) {
    ShaderStore.ShadersStoreWGSL[name] = shader;
}
/** @internal */
const rgbdDecodePixelShaderWGSL = { name, shader };

export { rgbdDecodePixelShaderWGSL };
//# sourceMappingURL=rgbdDecode.fragment-IgKiM3HZ.js.map
