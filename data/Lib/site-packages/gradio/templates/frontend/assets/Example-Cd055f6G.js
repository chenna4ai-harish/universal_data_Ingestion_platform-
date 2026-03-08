import { p as prop, e as init } from './i18n-CGlVUOvE.js';
import { M as push, t as template_effect, a as append, O as pop, P as flushSync, R as from_html, X as set_text, y as deep_read_state, w as untrack, Q as child, T as reset } from './index-Bq2njFOY.js';

var root = from_html(`<div style="display: none;"> </div>`);

function Example($$anchor, $$props) {
	push($$props, false);

	let value = prop($$props, 'value', 28, () => ({ visible: true, home_page_title: "Home" }));

	var $$exports = {
		get value() {
			return value();
		},

		set value($$value) {
			value($$value);
			flushSync();
		}
	};

	init();

	var div = root();
	var text = child(div);

	reset(div);

	template_effect(() => set_text(text, `Navbar config: visible=${(deep_read_state(value()), untrack(() => value().visible)) ?? ''}, home_page_title="${(
		deep_read_state(value()),
		untrack(() => value().home_page_title)
	) ?? ''}"`));

	append($$anchor, div);

	return pop($$exports);
}

export { Example as default };
//# sourceMappingURL=Example-Cd055f6G.js.map
