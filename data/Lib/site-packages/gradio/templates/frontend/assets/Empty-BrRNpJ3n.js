import { p as prop, s as slot, d as bind_this, b as set_class } from './i18n-CGlVUOvE.js';
import { M as push, Y as legacy_pre_effect, Z as get, _ as legacy_pre_effect_reset, $ as set, t as template_effect, a as append, O as pop, a0 as mutable_source, P as flushSync, Q as child, R as from_html, T as reset } from './index-Bq2njFOY.js';
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';

var root = from_html(`<div aria-label="Empty value"><div class="icon svelte-v95lt3"><!></div></div>`);

function Empty($$anchor, $$props) {
	push($$props, false);

	const parent_height = mutable_source();
	let size = prop($$props, 'size', 12, "small");
	let unpadded_box = prop($$props, 'unpadded_box', 12, false);
	let el = mutable_source();

	function compare_el_to_parent(el) {
		if (!el) return false;

		const { height: el_height } = el.getBoundingClientRect();
		const { height: parent_height } = el.parentElement?.getBoundingClientRect() || { height: el_height };

		return el_height > parent_height + 2;
	}

	legacy_pre_effect(() => (get(el)), () => {
		set(parent_height, compare_el_to_parent(get(el)));
	});

	legacy_pre_effect_reset();

	var $$exports = {
		get size() {
			return size();
		},

		set size($$value) {
			size($$value);
			flushSync();
		},

		get unpadded_box() {
			return unpadded_box();
		},

		set unpadded_box($$value) {
			unpadded_box($$value);
			flushSync();
		}
	};

	var div = root();
	let classes;
	var div_1 = child(div);
	var node = child(div_1);

	slot(node, $$props, 'default', {}, null);
	reset(div_1);
	reset(div);
	bind_this(div, ($$value) => set(el, $$value), () => get(el));

	template_effect(() => classes = set_class(div, 1, 'empty svelte-v95lt3', null, classes, {
		small: size() === "small",
		large: size() === "large",
		unpadded_box: unpadded_box(),
		small_parent: get(parent_height)
	}));

	append($$anchor, div);

	return pop($$exports);
}

export { Empty as E };
//# sourceMappingURL=Empty-BrRNpJ3n.js.map
