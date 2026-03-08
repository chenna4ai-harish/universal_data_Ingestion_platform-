import { S as ShaderStore } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';

// Do not edit.
const name = "ssaoCombinePixelShader";
const shader = `uniform sampler2D textureSampler;uniform sampler2D originalColor;uniform vec4 viewport;varying vec2 vUV;
#define CUSTOM_FRAGMENT_DEFINITIONS
void main(void) {
#define CUSTOM_FRAGMENT_MAIN_BEGIN
vec2 uv=viewport.xy+vUV*viewport.zw;vec4 ssaoColor=texture2D(textureSampler,uv);vec4 sceneColor=texture2D(originalColor,uv);gl_FragColor=sceneColor*ssaoColor;
#define CUSTOM_FRAGMENT_MAIN_END
}
`;
// Sideeffect
if (!ShaderStore.ShadersStore[name]) {
    ShaderStore.ShadersStore[name] = shader;
}
/** @internal */
const ssaoCombinePixelShader = { name, shader };

export { ssaoCombinePixelShader };
//# sourceMappingURL=ssaoCombine.fragment-C9ixryGn.js.map
