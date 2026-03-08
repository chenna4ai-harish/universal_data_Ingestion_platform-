import './async-D55cHugf.js';
import { a as attr, f as attr_class, g as attr_style, i as stringify, c as spread_props, s as slot } from './index-K3l_dLem.js';
import { S as Static } from './index3-DWk8IezT.js';
import './2-BYQShRaz.js';
import { G as Gradio } from './utils.svelte-DcXWObof.js';
import './escaping-CBnpiEl5.js';
import './context-DF4-UEpk.js';
import './index-Cg-Pg6j3.js';
import './MarkdownCode.svelte_svelte_type_style_lang-MeOh5TfF.js';
import './prism-python-3BtLB3SS.js';
import './IconButton-BOK4HpdV.js';
import './Clear-DH-TDCgr.js';
import './index5-BZVOFaHm.js';
import './dev-fallback-B-RpELjM.js';

function Index($$renderer, $$props) {
  $$renderer.component(($$renderer2) => {
    const get_dimension = (dimension_value) => {
      if (dimension_value === void 0) {
        return void 0;
      }
      if (typeof dimension_value === "number") {
        return dimension_value + "px";
      } else if (typeof dimension_value === "string") {
        return dimension_value;
      }
    };
    let { $$slots, $$events, ...props } = $$props;
    let gradio = new Gradio(props);
    $$renderer2.push(`<div${attr("id", gradio.shared.elem_id)}${attr_class(`row ${stringify(gradio.shared.elem_classes?.join(" "))}`, "svelte-7xavid", {
      "compact": gradio.props.variant === "compact",
      "panel": gradio.props.variant === "panel",
      "unequal-height": gradio.props.equal_height === false,
      "stretch": gradio.props.equal_height,
      "hide": !gradio.shared.visible,
      "grow-children": gradio.shared.scale && gradio.shared.scale >= 1
    })}${attr_style("", {
      height: get_dimension(gradio.props.height),
      "max-height": get_dimension(gradio.props.max_height),
      "min-height": get_dimension(gradio.props.min_height),
      "flex-grow": gradio.shared.scale
    })}>`);
    if (gradio.shared.loading_status && gradio.shared.loading_status.show_progress && gradio) {
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
//# sourceMappingURL=Index32-mvBiKlOx.js.map
