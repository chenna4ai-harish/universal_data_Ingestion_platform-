import { a9 as listen, aa as without_reactive_context } from './index-Bq2njFOY.js';

/**
 * @param {'innerWidth' | 'innerHeight' | 'outerWidth' | 'outerHeight'} type
 * @param {(size: number) => void} set
 */
function bind_window_size(type, set) {
	listen(window, ['resize'], () => without_reactive_context(() => set(window[type])));
}

export { bind_window_size as b };
//# sourceMappingURL=window-BVsmErEt.js.map
