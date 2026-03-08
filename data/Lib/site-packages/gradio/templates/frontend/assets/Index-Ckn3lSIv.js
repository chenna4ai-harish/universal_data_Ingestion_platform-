import { s as slot, r as rest_props } from './i18n-CGlVUOvE.js';
import { M as push, a3 as comment, N as first_child, a as append, O as pop } from './index-Bq2njFOY.js';
import { G as Gradio } from './utils.svelte-Bi9ASwv9.js';
import { B as BaseForm } from './BaseForm-BF0VUPNl.js';

function Index($$anchor, $$props) {
	push($$props, true);

	let props = rest_props($$props, ['$$slots', '$$events', '$$legacy']);
	const gradio = new Gradio(props);

	BaseForm($$anchor, {
		get visible() {
			return gradio.shared.visible;
		},

		get scale() {
			return gradio.shared.scale;
		},

		get min_width() {
			return gradio.shared.min_width;
		},

		children: ($$anchor, $$slotProps) => {
			var fragment_1 = comment();
			var node = first_child(fragment_1);

			slot(node, $$props, 'default', {}, null);
			append($$anchor, fragment_1);
		},
		$$slots: { default: true }
	});

	pop();
}

export { BaseForm, Index as default };
//# sourceMappingURL=Index-Ckn3lSIv.js.map
