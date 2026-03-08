import { g as spread_props, r as rest_props, s as slot } from './i18n-CGlVUOvE.js';
import { M as push, O as pop, a3 as comment, N as first_child, a as append } from './index-Bq2njFOY.js';
import { G as Gradio } from './utils.svelte-Bi9ASwv9.js';
import { B as BaseColumn } from './Index.svelte_svelte_type_style_lang-B7TVXWMD.js';
import './index-1GCmrSlD.js';
import './StreamingBar.svelte_svelte_type_style_lang-DWGn5tT_.js';
import './html-Bwif0JPw.js';
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
import './snippet-BG7qkY_1.js';
import './MarkdownCode.svelte_svelte_type_style_lang-B29zDiob.js';
import './prism-python-NrKIQnfs.js';
import './Clear-C8Tfa7QP.js';

function Index($$anchor, $$props) {
	push($$props, true);

	let props = rest_props($$props, ['$$slots', '$$events', '$$legacy']);
	const gradio = new Gradio(props);

	BaseColumn($$anchor, spread_props(() => gradio.shared, {
		children: ($$anchor, $$slotProps) => {
			var fragment_1 = comment();
			var node = first_child(fragment_1);

			slot(node, $$props, 'default', {}, null);
			append($$anchor, fragment_1);
		},
		$$slots: { default: true }
	}));

	pop();
}

export { BaseColumn, Index as default };
//# sourceMappingURL=Index-BnPF6xy8.js.map
