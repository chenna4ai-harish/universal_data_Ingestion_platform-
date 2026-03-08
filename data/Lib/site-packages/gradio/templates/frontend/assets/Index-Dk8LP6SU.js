import { r as rest_props } from './i18n-CGlVUOvE.js';
import { M as push, a4 as text, t as template_effect, a as append, Z as get, O as pop, a2 as user_derived, X as set_text, a5 as next } from './index-Bq2njFOY.js';
import { G as Gradio } from './utils.svelte-Bi9ASwv9.js';
import { B as Button } from './Button-By21KHDn.js';
import './snippet-BG7qkY_1.js';
import './Image-XuMqTTwB.js';
import './misc-D8a4ZbMA.js';
/* empty css                                             */
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
import './MarkdownCode.svelte_svelte_type_style_lang-B29zDiob.js';
import './prism-python-NrKIQnfs.js';
/* empty css                                                    */

function Index($$anchor, $$props) {
	push($$props, true);

	let _props = rest_props($$props, ['$$slots', '$$events', '$$legacy']);
	const gradio = new Gradio(_props);

	function handle_click() {
		gradio.dispatch("click");
	}

	{
		let $0 = user_derived(() => !gradio.shared.interactive);

		Button($$anchor, {
			get value() {
				return gradio.props.value;
			},

			get variant() {
				return gradio.props.variant;
			},

			get elem_id() {
				return gradio.shared.elem_id;
			},

			get elem_classes() {
				return gradio.shared.elem_classes;
			},

			get size() {
				return gradio.props.size;
			},

			get scale() {
				return gradio.shared.scale;
			},

			get link() {
				return gradio.props.link;
			},

			get icon() {
				return gradio.props.icon;
			},

			get min_width() {
				return gradio.shared.min_width;
			},

			get visible() {
				return gradio.shared.visible;
			},

			get disabled() {
				return get($0);
			},

			get link_target() {
				return gradio.props.link_target;
			},
			onclick: handle_click,
			children: ($$anchor, $$slotProps) => {
				next();

				var text$1 = text();

				template_effect(() => set_text(text$1, gradio.props.value ?? ""));
				append($$anchor, text$1);
			},
			$$slots: { default: true }
		});
	}

	pop();
}

export { Button as BaseButton, Index as default };
//# sourceMappingURL=Index-Dk8LP6SU.js.map
