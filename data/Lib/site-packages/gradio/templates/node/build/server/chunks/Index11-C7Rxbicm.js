import './async-D55cHugf.js';
import { c as spread_props, s as slot } from './index-K3l_dLem.js';
import './2-BYQShRaz.js';
import { G as Gradio } from './utils.svelte-DcXWObof.js';
import { B as BaseColumn } from './Index.svelte_svelte_type_style_lang-Ck29Kh-W.js';
import './escaping-CBnpiEl5.js';
import './context-DF4-UEpk.js';
import './index5-BZVOFaHm.js';
import './dev-fallback-B-RpELjM.js';
import './index-Cg-Pg6j3.js';
import './index3-DWk8IezT.js';
import './MarkdownCode.svelte_svelte_type_style_lang-MeOh5TfF.js';
import './prism-python-3BtLB3SS.js';
import './IconButton-BOK4HpdV.js';
import './Clear-DH-TDCgr.js';

function Index($$renderer, $$props) {
  $$renderer.component(($$renderer2) => {
    let { $$slots, $$events, ...props } = $$props;
    const gradio = new Gradio(props);
    BaseColumn($$renderer2, spread_props([
      gradio.shared,
      {
        children: ($$renderer3) => {
          $$renderer3.push(`<!--[-->`);
          slot($$renderer3, $$props, "default", {}, null);
          $$renderer3.push(`<!--]-->`);
        },
        $$slots: { default: true }
      }
    ]));
  });
}

export { BaseColumn, Index as default };
//# sourceMappingURL=Index11-C7Rxbicm.js.map
