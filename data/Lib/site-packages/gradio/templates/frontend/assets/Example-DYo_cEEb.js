import { p as prop, e as init, d as bind_this, b as set_class } from './i18n-CGlVUOvE.js';
import { M as push, a8 as onMount, t as template_effect, y as deep_read_state, w as untrack, X as set_text, a as append, O as pop, P as flushSync, R as from_html, Q as child, $ as set, a0 as mutable_source, Z as get, T as reset } from './index-Bq2njFOY.js';
import { b as bind_element_size } from './size-CWi277d_.js';
/* empty css                                               */

var root = from_html(`<div> </div>`);

function Example($$anchor, $$props) {
	push($$props, false);

	let value = prop($$props, 'value', 12);
	let type = prop($$props, 'type', 12);
	let selected = prop($$props, 'selected', 12, false);
	let size = mutable_source();
	let el = mutable_source();

	function set_styles(element, el_width) {
		element.style.setProperty("--local-text-width", `${el_width && el_width < 150 ? el_width : 200}px`);
		element.style.whiteSpace = "unset";
	}

	function truncate_text(text, max_length = 60) {
		if (!text) return "";

		const str = String(text);

		if (str.length <= max_length) return str;

		return str.slice(0, max_length) + "...";
	}

	onMount(() => {
		set_styles(get(el), get(size));
	});

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
	var text_1 = child(div, true);

	reset(div);
	bind_this(div, ($$value) => set(el, $$value), () => get(el));

	template_effect(
		($0) => {
			classes = set_class(div, 1, 'svelte-xxobeb', null, classes, {
				table: type() === "table",
				gallery: type() === "gallery",
				selected: selected()
			});

			set_text(text_1, $0);
		},
		[
			() => (
				deep_read_state(value()),
				untrack(() => truncate_text(value()))
			)
		]
	);

	bind_element_size(div, 'clientWidth', ($$value) => set(size, $$value));
	append($$anchor, div);

	return pop($$exports);
}

export { Example as default };
//# sourceMappingURL=Example-DYo_cEEb.js.map
