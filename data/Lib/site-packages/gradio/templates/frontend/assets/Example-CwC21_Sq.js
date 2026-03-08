import { p as prop, i as if_block, b as set_class } from './i18n-CGlVUOvE.js';
import { M as push, a3 as comment, N as first_child, a as append, O as pop, Q as child, $ as set, Z as get, T as reset, t as template_effect, R as from_html, ab as state, a2 as user_derived } from './index-Bq2njFOY.js';
import { V as Video } from './Video-e9Pn2mbW.js';
import './snippet-BG7qkY_1.js';
import './actions-CgRQ2lHA.js';

var root_2 = from_html(`<div><!></div>`);

function Example($$anchor, $$props) {
	push($$props, true);

	let selected = prop($$props, 'selected', 3, false),
		value = prop($$props, 'value', 3, null);

	let video = state(void 0);

	async function init() {
		if (!get(video)) return;

		get(video).muted = true;
		get(video).playsInline = true;
		get(video).controls = false;
		get(video).setAttribute("muted", "");
		await get(video).play();
		get(video).pause();
	}

	var fragment = comment();
	var node = first_child(fragment);

	{
		var consequent_1 = ($$anchor) => {
			var fragment_1 = comment();
			var node_1 = first_child(fragment_1);

			{
				var consequent = ($$anchor) => {
					var div = root_2();
					let classes;
					var node_2 = child(div);

					{
						let $0 = user_derived(() => value()?.url);

						Video(node_2, {
							muted: true,
							playsinline: true,
							onloadeddata: init,
							onmouseover: () => get(video)?.play(),
							onmouseout: () => get(video)?.pause(),
							get src() {
								return get($0);
							},
							is_stream: false,
							get loop() {
								return $$props.loop;
							},

							get node() {
								return get(video);
							},

							set node($$value) {
								set(video, $$value, true);
							}
						});
					}

					reset(div);

					template_effect(() => classes = set_class(div, 1, 'container svelte-1nl1glk', null, classes, {
						table: $$props.type === "table",
						gallery: $$props.type === "gallery",
						selected: selected()
					}));

					append($$anchor, div);
				};

				if_block(node_1, ($$render) => {
					$$render(consequent);
				});
			}

			append($$anchor, fragment_1);
		};

		if_block(node, ($$render) => {
			if (value()) $$render(consequent_1);
		});
	}

	append($$anchor, fragment);
	pop();
}

export { Example as default };
//# sourceMappingURL=Example-CwC21_Sq.js.map
