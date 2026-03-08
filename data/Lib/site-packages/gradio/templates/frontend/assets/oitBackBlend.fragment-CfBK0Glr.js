import { S as ShaderStore } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';

// Do not edit.
const name = "oitBackBlendPixelShader";
const shader = `var uBackColor: texture_2d<f32>;@fragment
fn main(input: FragmentInputs)->FragmentOutputs {fragmentOutputs.color=textureLoad(uBackColor,vec2i(fragmentInputs.position.xy),0);if (fragmentOutputs.color.a==0.0) {discard;}}
`;
// Sideeffect
if (!ShaderStore.ShadersStoreWGSL[name]) {
    ShaderStore.ShadersStoreWGSL[name] = shader;
}
/** @internal */
const oitBackBlendPixelShaderWGSL = { name, shader };

export { oitBackBlendPixelShaderWGSL };
//# sourceMappingURL=oitBackBlend.fragment-CfBK0Glr.js.map
