import { s as styles_default, b as stateRenderer_v3_unified_default, a as stateDiagram_default, S as StateDB } from './chunk-DI55MBZ5-D6RL0xty-BTVmwXfy.js';
import { _ as __name } from './mermaid.core-vMMZVCDT-alxhRXoZ.js';
import './chunk-55IACEB6-DFDtoedX-BGSQphJz.js';
import './chunk-QN33PNHL-ChAeawbz-BLoTvni4.js';
import './index-Bq2njFOY.js';
import './Index-BOG-LPoW.js';
import './i18n-CGlVUOvE.js';
import './utils.svelte-Bi9ASwv9.js';
import './index-DnoGeqVF.js';
import './dsv-BhAd467f.js';
import './props-o1aFOJaB.js';
import './misc-D8a4ZbMA.js';
import './index-By61_kAe.js';
import './Upload-xR3P-2U5.js';
import './snippet-BG7qkY_1.js';
import './actions-CgRQ2lHA.js';
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
import './MarkdownCode.svelte_svelte_type_style_lang-B29zDiob.js';
import './prism-python-NrKIQnfs.js';
import './html-Bwif0JPw.js';
import './input-CXpCw23l.js';
import './event-modifiers-DanhKw3_.js';
import './MarkdownCode-BYKuypS7.js';
import './StreamingBar.svelte_svelte_type_style_lang-DWGn5tT_.js';
import './Checkbox-dsIC6373.js';
import './size-CWi277d_.js';
import './Check-BKMHx_DF.js';
import './DropdownArrow-3lFHYtTD.js';
import './Copy-BQlJe6-D.js';
import './FullscreenButton-C6RgeACK.js';
import './Maximize-2N5airbC.js';
import './Example-CSyNUPmz.js';

var diagram = {
  parser: stateDiagram_default,
  get db() {
    return new StateDB(2);
  },
  renderer: stateRenderer_v3_unified_default,
  styles: styles_default,
  init: /* @__PURE__ */ __name((cnf) => {
    if (!cnf.state) {
      cnf.state = {};
    }
    cnf.state.arrowMarkerAbsolute = cnf.arrowMarkerAbsolute;
  }, "init")
};

export { diagram };
//# sourceMappingURL=stateDiagram-v2-4FDKWEC3-GtMZ-gfW-DCar7mgq.js.map
