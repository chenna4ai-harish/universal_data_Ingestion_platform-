import { p as prop, e as init, b as set_class } from './i18n-CGlVUOvE.js';
import { M as push, t as template_effect, a as append, O as pop, P as flushSync, Q as child, R as from_html, Z as get, ae as derived_safe_equal, y as deep_read_state, w as untrack, T as reset } from './index-Bq2njFOY.js';
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
import './MarkdownCode.svelte_svelte_type_style_lang-B29zDiob.js';
import { I as Image } from './Image-XuMqTTwB.js';
/* empty css                                                    */
import './StreamingBar.svelte_svelte_type_style_lang-DWGn5tT_.js';
/* empty css                                                     */
import './Upload-xR3P-2U5.js';
/* empty css                                               */
import './snippet-BG7qkY_1.js';
import './prism-python-NrKIQnfs.js';
import './misc-D8a4ZbMA.js';
/* empty css                                             */
import './html-Bwif0JPw.js';
import './actions-CgRQ2lHA.js';

var root = from_html(`<div><!></div>`);

function Example($$anchor, $$props) {
	push($$props, false);

	let value = prop($$props, 'value', 12);
	let type = prop($$props, 'type', 12);
	let selected = prop($$props, 'selected', 12, false);

	var $$exports = {
		get value() {
			return value();
		},

		set value($$value) {
			value($$value);
			flushSync();
		},

		get type() {
			return type();
		},

		set type($$value) {
			type($$value);
			flushSync();
		},

		get selected() {
			return selected();
		},

		set selected($$value) {
			selected($$value);
			flushSync();
		}
	};

	init();

	var div = root();
	let classes;
	var node = child(div);

	{
		let $0 = derived_safe_equal(() => (
			deep_read_state(value()),
			untrack(() => value().composite?.url || value().background?.url)
		));

		Image(node, {
			get src() {
				return get($0);
			},
			alt: ''
		});
	}

	reset(div);

	template_effect(() => classes = set_class(div, 1, 'container svelte-ous74z', null, classes, {
		table: type() === "table",
		gallery: type() === "gallery",
		selected: selected()
	}));

	append($$anchor, div);

	return pop($$exports);
}

export { Example as default };
//# sourceMappingURL=Example-YlQBeDNh.js.map
