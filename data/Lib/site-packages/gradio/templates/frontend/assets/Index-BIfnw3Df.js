import { p as prop, s as slot } from './i18n-CGlVUOvE.js';
import { M as push, a3 as comment, N as first_child, a as append, O as pop, P as flushSync } from './index-Bq2njFOY.js';
import { B as Block } from './Block-DNK-JdLJ.js';
import './MarkdownCode.svelte_svelte_type_style_lang-B29zDiob.js';
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
import './prism-python-NrKIQnfs.js';
import './snippet-BG7qkY_1.js';

function Index($$anchor, $$props) {
	push($$props, false);

	let elem_id = prop($$props, 'elem_id', 12);
	let elem_classes = prop($$props, 'elem_classes', 12);
	let visible = prop($$props, 'visible', 12, true);

	var $$exports = {
		get elem_id() {
			return elem_id();
		},

		set elem_id($$value) {
			elem_id($$value);
			flushSync();
		},

		get elem_classes() {
			return elem_classes();
		},

		set elem_classes($$value) {
			elem_classes($$value);
			flushSync();
		},

		get visible() {
			return visible();
		},

		set visible($$value) {
			visible($$value);
			flushSync();
		}
	};

	Block($$anchor, {
		get elem_id() {
			return elem_id();
		},

		get elem_classes() {
			return elem_classes();
		},

		get visible() {
			return visible();
		},
		explicit_call: true,
		children: ($$anchor, $$slotProps) => {
			var fragment_1 = comment();
			var node = first_child(fragment_1);

			slot(node, $$props, 'default', {}, null);
			append($$anchor, fragment_1);
		},
		$$slots: { default: true }
	});

	return pop($$exports);
}

export { Index as default };
//# sourceMappingURL=Index-BIfnw3Df.js.map
