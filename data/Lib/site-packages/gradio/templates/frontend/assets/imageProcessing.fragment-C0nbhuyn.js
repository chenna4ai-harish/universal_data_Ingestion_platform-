import { S as ShaderStore } from './index-BeVTUo08.js';
import './imageProcessingFunctions-BkaOMyZe.js';
import './helperFunctions-KDBcQUkc.js';
import './index-Bq2njFOY.js';

// Do not edit.
const name = "imageProcessingPixelShader";
const shader = `varying vec2 vUV;uniform sampler2D textureSampler;
#include<imageProcessingDeclaration>
#include<helperFunctions>
#include<imageProcessingFunctions>
#define CUSTOM_FRAGMENT_DEFINITIONS
void main(void)
{vec4 result=texture2D(textureSampler,vUV);result.rgb=max(result.rgb,vec3(0.));
#ifdef IMAGEPROCESSING
#ifndef FROMLINEARSPACE
result.rgb=toLinearSpace(result.rgb);
#endif
result=applyImageProcessing(result);
#else
#ifdef FROMLINEARSPACE
result=applyImageProcessing(result);
#endif
#endif
gl_FragColor=result;}`;
// Sideeffect
if (!ShaderStore.ShadersStore[name]) {
    ShaderStore.ShadersStore[name] = shader;
}
/** @internal */
const imageProcessingPixelShader = { name, shader };

export { imageProcessingPixelShader };
//# sourceMappingURL=imageProcessing.fragment-C0nbhuyn.js.map
