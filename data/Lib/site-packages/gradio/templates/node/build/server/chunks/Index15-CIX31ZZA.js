import './async-D55cHugf.js';
import { a as attr, f as attr_class, j as clsx } from './index-K3l_dLem.js';
import './2-BYQShRaz.js';
import { G as Gradio } from './utils.svelte-DcXWObof.js';
import './escaping-CBnpiEl5.js';
import './context-DF4-UEpk.js';
import './index5-BZVOFaHm.js';
import './dev-fallback-B-RpELjM.js';
import './index-Cg-Pg6j3.js';

function Index($$renderer, $$props) {
  $$renderer.component(($$renderer2) => {
    const { $$slots, $$events, ...props } = $$props;
    const gradio = new Gradio(props);
    (() => {
      return {
        visible: gradio.shared.visible,
        main_page_name: gradio.props.main_page_name ?? "Home",
        value: gradio.props.value
      };
    })();
    $$renderer2.push(`<div style="display: none;"${attr("id", gradio.shared.elem_id)}${attr_class(clsx(gradio.shared.elem_classes.join(" ")))}></div>`);
  });
}

export { Index as default };
//# sourceMappingURL=Index15-CIX31ZZA.js.map
