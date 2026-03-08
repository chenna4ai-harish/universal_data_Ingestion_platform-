import './async-D55cHugf.js';
import { c as spread_props, a as attr, e as ensure_array_like, f as attr_class } from './index-K3l_dLem.js';
import './2-BYQShRaz.js';
import { G as Gradio } from './utils.svelte-DcXWObof.js';
import { B as Block } from './Block-qDbnR9DW.js';
import { B as BlockTitle } from './BlockTitle-CIA4Cwld.js';
import './MarkdownCode.svelte_svelte_type_style_lang-MeOh5TfF.js';
import { I as IconButtonWrapper } from './IconButtonWrapper-BSVqsNEI.js';
import { S as Static } from './index3-DWk8IezT.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';
import './context-DF4-UEpk.js';
import './index5-BZVOFaHm.js';
import './dev-fallback-B-RpELjM.js';
import './index-Cg-Pg6j3.js';
import './Info-BG_G1YGk.js';
import './MarkdownCode-DlJpAOAo.js';
import './index35-D4X1IPn5.js';
import 'path';
import 'url';
import 'fs';
import './html-CfyvkLET.js';
import './prism-python-3BtLB3SS.js';
import './IconButton-BOK4HpdV.js';
import './Clear-DH-TDCgr.js';

function Index($$renderer, $$props) {
  $$renderer.component(($$renderer2) => {
    let { $$slots, $$events, ...props } = $$props;
    let gradio = new Gradio(props);
    let select_all_state = (() => {
      const all_values = gradio.props.choices.map(([, internal_value]) => internal_value);
      if (gradio.props.value.length === 0) return "unchecked";
      if (gradio.props.value.length === all_values.length) return "checked";
      return "indeterminate";
    })();
    let disabled = !gradio.shared.interactive;
    gradio.props.value;
    Block($$renderer2, {
      visible: gradio.shared.visible,
      elem_id: gradio.shared.elem_id,
      elem_classes: gradio.shared.elem_classes,
      type: "fieldset",
      container: gradio.shared.container,
      scale: gradio.shared.scale,
      min_width: gradio.shared.min_width,
      children: ($$renderer3) => {
        Static($$renderer3, spread_props([
          { autoscroll: gradio.shared.autoscroll, i18n: gradio.i18n },
          gradio.shared.loading_status,
          {
            on_clear_status: () => gradio.dispatch("clear_status", gradio.shared.loading_status)
          }
        ]));
        $$renderer3.push(`<!----> `);
        if (gradio.shared.show_label && gradio.props.buttons && gradio.props.buttons.length > 0) {
          $$renderer3.push("<!--[-->");
          IconButtonWrapper($$renderer3, {
            buttons: gradio.props.buttons,
            on_custom_button_click: (id) => {
              gradio.dispatch("custom_button_click", { id });
            }
          });
        } else {
          $$renderer3.push("<!--[!-->");
        }
        $$renderer3.push(`<!--]--> `);
        BlockTitle($$renderer3, {
          show_label: gradio.shared.show_label || gradio.props.show_select_all && gradio.shared.interactive,
          info: gradio.props.info,
          children: ($$renderer4) => {
            if (gradio.props.show_select_all && gradio.shared.interactive) {
              $$renderer4.push("<!--[-->");
              $$renderer4.push(`<div class="select-all-container svelte-yb2gcx"><label class="select-all-label svelte-yb2gcx"><input class="select-all-checkbox svelte-yb2gcx"${attr("checked", select_all_state === "checked", true)}${attr("indeterminate", select_all_state === "indeterminate", true)} type="checkbox" title="Select/Deselect All"/></label> <button type="button" class="label-text svelte-yb2gcx">${escape_html(gradio.shared.show_label ? gradio.shared.label : "Select All")}</button></div>`);
            } else {
              $$renderer4.push("<!--[!-->");
              if (gradio.shared.show_label) {
                $$renderer4.push("<!--[-->");
                $$renderer4.push(`${escape_html(gradio.shared.label || gradio.i18n("checkbox.checkbox_group"))}`);
              } else {
                $$renderer4.push("<!--[!-->");
              }
              $$renderer4.push(`<!--]-->`);
            }
            $$renderer4.push(`<!--]-->`);
          },
          $$slots: { default: true }
        });
        $$renderer3.push(`<!----> <div class="wrap svelte-yb2gcx" data-testid="checkbox-group"><!--[-->`);
        const each_array = ensure_array_like(gradio.props.choices);
        for (let i = 0, $$length = each_array.length; i < $$length; i++) {
          let [display_value, internal_value] = each_array[i];
          $$renderer3.push(`<label${attr_class("svelte-yb2gcx", void 0, {
            "disabled": disabled,
            "selected": gradio.props.value.includes(internal_value)
          })}><input${attr("disabled", disabled, true)}${attr("checked", gradio.props.value.includes(internal_value), true)} type="checkbox"${attr("name", internal_value?.toString())}${attr("title", internal_value?.toString())} class="svelte-yb2gcx"/> <span class="ml-2 svelte-yb2gcx">${escape_html(display_value)}</span></label>`);
        }
        $$renderer3.push(`<!--]--></div>`);
      },
      $$slots: { default: true }
    });
  });
}

export { Index as default };
//# sourceMappingURL=Index23-BX6o4dmM.js.map
