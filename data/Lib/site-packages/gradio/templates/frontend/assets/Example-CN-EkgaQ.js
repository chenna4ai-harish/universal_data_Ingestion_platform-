import { p as prop, i as if_block } from './i18n-CGlVUOvE.js';
import { M as push, a3 as comment, N as first_child, a as append, O as pop, P as flushSync, a4 as text, t as template_effect, X as set_text } from './index-Bq2njFOY.js';

function Example($$anchor, $$props) {
	push($$props, false);

	let title = prop($$props, 'title', 12);
	let x = prop($$props, 'x', 12);
	let y = prop($$props, 'y', 12);

	var $$exports = {
		get title() {
			return title();
		},

		set title($$value) {
			title($$value);
			flushSync();
		},

		get x() {
			return x();
		},

		set x($$value) {
			x($$value);
			flushSync();
		},

		get y() {
			return y();
		},

		set y($$value) {
			y($$value);
			flushSync();
		}
	};

	var fragment = comment();
	var node = first_child(fragment);

	{
		var consequent = ($$anchor) => {
			var text$1 = text();

			template_effect(() => set_text(text$1, title()));
			append($$anchor, text$1);
		};

		var alternate = ($$anchor) => {
			var text_1 = text();

			template_effect(() => set_text(text_1, `${x() ?? ''} x ${y() ?? ''}`));
			append($$anchor, text_1);
		};

		if_block(node, ($$render) => {
			if (title()) $$render(consequent); else $$render(alternate, false);
		});
	}

	append($$anchor, fragment);

	return pop($$exports);
}

export { Example as default };
//# sourceMappingURL=Example-CN-EkgaQ.js.map
