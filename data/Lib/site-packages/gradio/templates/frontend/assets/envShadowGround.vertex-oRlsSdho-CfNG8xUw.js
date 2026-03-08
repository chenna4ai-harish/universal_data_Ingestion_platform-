import { S as ShaderStore } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';

// Do not edit.
const name = "envShadowGroundVertexShader";
const shader = `attribute position: vec3f;attribute uv: vec2f;uniform viewProjection: mat4x4f;uniform worldViewProjection: mat4x4f;varying vUV: vec2f;@vertex
fn main(input : VertexInputs)->FragmentInputs {vertexOutputs.position=uniforms.worldViewProjection*vec4f(input.position,1.0);vertexOutputs.vUV=input.uv;}`;
// Sideeffect
if (!ShaderStore.ShadersStoreWGSL[name]) {
    ShaderStore.ShadersStoreWGSL[name] = shader;
}
/** @internal */
const envShadowGroundVertexShaderWGSL = { name, shader };

export { envShadowGroundVertexShaderWGSL };
//# sourceMappingURL=envShadowGround.vertex-oRlsSdho-CfNG8xUw.js.map
