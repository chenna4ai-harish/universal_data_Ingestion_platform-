import { S as ShaderStore } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';

// Do not edit.
const name = "envShadowGroundPixelShader";
const shader = `precision highp float;uniform sampler2D shadowTexture;uniform vec2 renderTargetSize;uniform float shadowOpacity;varying vec2 vUV;void main(void) {float uvBasedOpacity=clamp(length(vUV*vec2(2.0)-vec2(1.0)),0.0,1.0);uvBasedOpacity=uvBasedOpacity*uvBasedOpacity;uvBasedOpacity=1.0-uvBasedOpacity;vec2 screenUv=gl_FragCoord.xy/renderTargetSize;vec3 shadowValue=texture2D(shadowTexture,screenUv).rrr;float totalOpacity=shadowOpacity*uvBasedOpacity;vec3 invertedShadowValue=vec3(1.0)-shadowValue;gl_FragColor.rgb=shadowValue;gl_FragColor.a=invertedShadowValue.r*totalOpacity;}`;
// Sideeffect
if (!ShaderStore.ShadersStore[name]) {
    ShaderStore.ShadersStore[name] = shader;
}
/** @internal */
const envShadowGroundPixelShader = { name, shader };

export { envShadowGroundPixelShader };
//# sourceMappingURL=envShadowGround.fragment-L1sPCORS-CQB_n7fv.js.map
