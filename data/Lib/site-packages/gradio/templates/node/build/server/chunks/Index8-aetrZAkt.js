import './async-D55cHugf.js';
import { e as escape_html } from './escaping-CBnpiEl5.js';
import './2-BYQShRaz.js';
import { G as Gradio } from './utils.svelte-DcXWObof.js';
import { B as Button } from './Button-Xyl1EeiK.js';
import './index5-BZVOFaHm.js';
import './dev-fallback-B-RpELjM.js';
import './index-Cg-Pg6j3.js';
import './index-K3l_dLem.js';
import './context-DF4-UEpk.js';
import './Image-CZw3rP1w.js';
import './MarkdownCode.svelte_svelte_type_style_lang-MeOh5TfF.js';
import './prism-python-3BtLB3SS.js';

function Index($$renderer, $$props) {
  $$renderer.component(($$renderer2) => {
    let { $$slots, $$events, ..._props } = $$props;
    const gradio = new Gradio(_props);
    function handle_click() {
      gradio.dispatch("click");
    }
    Button($$renderer2, {
      value: gradio.props.value,
      variant: gradio.props.variant,
      elem_id: gradio.shared.elem_id,
      elem_classes: gradio.shared.elem_classes,
      size: gradio.props.size,
      scale: gradio.shared.scale,
      link: gradio.props.link,
      icon: gradio.props.icon,
      min_width: gradio.shared.min_width,
      visible: gradio.shared.visible,
      disabled: !gradio.shared.interactive,
      link_target: gradio.props.link_target,
      onclick: handle_click,
      children: ($$renderer3) => {
        $$renderer3.push(`<!---->${escape_html(gradio.props.value ?? "")}`);
      }
    });
  });
}

export { Button as BaseButton, Index as default };
//# sourceMappingURL=Index8-aetrZAkt.js.map
