import { p as prop, e as init, b as set_class } from './i18n-CGlVUOvE.js';
import { M as push, t as template_effect, y as deep_read_state, w as untrack, X as set_text, a as append, O as pop, P as flushSync, R as from_html, Q as child, T as reset } from './index-Bq2njFOY.js';

var root = from_html(`<div> </div>`);

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
	var text = child(div, true);

	reset(div);

	template_effect(
		($0) => {
			classes = set_class(div, 1, 'svelte-164n2qi', null, classes, {
				table: type() === "table",
				gallery: type() === "gallery",
				selected: selected()
			});

			set_text(text, $0);
		},
		[
			() => (
				deep_read_state(value()),
				untrack(() => value() !== null ? value().toLocaleString() : "")
			)
		]
	);

	append($$anchor, div);

	return pop($$exports);
}

export { Example as default };
//# sourceMappingURL=Example-lKnDE0-B.js.map
