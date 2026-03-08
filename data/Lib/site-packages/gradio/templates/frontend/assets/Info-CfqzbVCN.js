import { p as prop } from './i18n-CGlVUOvE.js';
import { M as push, a as append, O as pop, P as flushSync, Q as child, R as from_html, T as reset } from './index-Bq2njFOY.js';
import { M as MarkdownCode } from './MarkdownCode-BYKuypS7.js';
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';

var root = from_html(`<div class="svelte-9hc4ua"><!></div>`);

function Info($$anchor, $$props) {
	push($$props, false);

	let info = prop($$props, 'info', 12);

	var $$exports = {
		get info() {
			return info();
		},

		set info($$value) {
			info($$value);
			flushSync();
		}
	};

	var div = root();
	var node = child(div);

	MarkdownCode(node, {
		get message() {
			return info();
		},
		sanitize_html: true
	});

	reset(div);
	append($$anchor, div);

	return pop($$exports);
}

export { Info as I };
//# sourceMappingURL=Info-CfqzbVCN.js.map
