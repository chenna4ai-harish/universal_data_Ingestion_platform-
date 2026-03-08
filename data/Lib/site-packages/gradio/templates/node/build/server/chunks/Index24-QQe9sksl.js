import './async-D55cHugf.js';
import { a as attr, f as attr_class, i as stringify, c as spread_props, s as slot } from './index-K3l_dLem.js';
import './2-BYQShRaz.js';
import { G as Gradio } from './utils.svelte-DcXWObof.js';
import { S as Static } from './index3-DWk8IezT.js';
import './escaping-CBnpiEl5.js';
import './context-DF4-UEpk.js';
import './index5-BZVOFaHm.js';
import './dev-fallback-B-RpELjM.js';
import './index-Cg-Pg6j3.js';
import './MarkdownCode.svelte_svelte_type_style_lang-MeOh5TfF.js';
import './prism-python-3BtLB3SS.js';
import './IconButton-BOK4HpdV.js';
import './Clear-DH-TDCgr.js';

function Index($$renderer, $$props) {
  $$renderer.component(($$renderer2) => {
    const { $$slots, $$events, ...props } = $$props;
    const gradio = new Gradio(props);
    $$renderer2.push(`<div${attr("id", gradio.shared.elem_id)}${attr_class(`draggable ${stringify((gradio.shared.elem_classes || []).join(" "))}`, "svelte-1kr8imm", {
      "hide": !gradio.shared.visible,
      "horizontal": gradio.props.orientation === "row",
      "vertical": gradio.props.orientation === "column"
    })} role="region" aria-label="Draggable items container">`);
    if (gradio.shared.loading_status && gradio.props.show_progress) {
      $$renderer2.push("<!--[-->");
      Static($$renderer2, spread_props([
        { autoscroll: gradio.shared.autoscroll, i18n: gradio.i18n },
        gradio.shared.loading_status,
        {
          status: gradio.shared.loading_status ? gradio.shared.loading_status.status == "pending" ? "generating" : gradio.shared.loading_status.status : null
        }
      ]));
    } else {
      $$renderer2.push("<!--[!-->");
    }
    $$renderer2.push(`<!--]--> <!--[-->`);
    slot($$renderer2, $$props, "default", {}, null);
    $$renderer2.push(`<!--]--></div>`);
  });
}

export { Index as default };
//# sourceMappingURL=Index24-QQe9sksl.js.map
