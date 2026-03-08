import { s as styles_default, c as classRenderer_v3_unified_default, a as classDiagram_default, C as ClassDB } from './chunk-B4BG7PRW-Bn9HEt3A.js';
import { _ as __name } from './mermaid.core-BKdFzeqO.js';
import './chunk-FMBD7UC4-mBtdZM1e.js';
import './chunk-55IACEB6-MvRTnDZn.js';
import './select-k8gDf_61.js';
import './chunk-QN33PNHL-abXd6ObZ.js';
import './index-Bq2njFOY.js';
import './i18n-CGlVUOvE.js';
import './step-TZOpqHBK.js';
import './dispatch-tQmgj1It.js';

// src/diagrams/class/classDiagram-v2.ts
var diagram = {
  parser: classDiagram_default,
  get db() {
    return new ClassDB();
  },
  renderer: classRenderer_v3_unified_default,
  styles: styles_default,
  init: /* @__PURE__ */ __name((cnf) => {
    if (!cnf.class) {
      cnf.class = {};
    }
    cnf.class.arrowMarkerAbsolute = cnf.arrowMarkerAbsolute;
  }, "init")
};

export { diagram };
//# sourceMappingURL=classDiagram-v2-WZHVMYZB-CA_aOjiW.js.map
