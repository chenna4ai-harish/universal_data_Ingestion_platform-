import { S as ShaderStore } from './index-BeVTUo08.js';
import './index-Bq2njFOY.js';

// Do not edit.
const name = "shadowMapFragmentSoftTransparentShadow";
const shader = `#if SM_SOFTTRANSPARENTSHADOW==1
if ((bayerDither8(floor(((fragmentInputs.position.xy)%(8.0)))))/64.0>=uniforms.softTransparentShadowSM.x*alpha) {discard;}
#endif
`;
// Sideeffect
if (!ShaderStore.IncludesShadersStoreWGSL[name]) {
    ShaderStore.IncludesShadersStoreWGSL[name] = shader;
}
/** @internal */
const shadowMapFragmentSoftTransparentShadowWGSL = { name, shader };

export { shadowMapFragmentSoftTransparentShadowWGSL };
//# sourceMappingURL=shadowMapFragmentSoftTransparentShadow-PZbMcelz.js.map
