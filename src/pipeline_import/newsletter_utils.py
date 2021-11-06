#! /usr/bin/env python3


def get_color_stats_text(color_stats):
    color_stats.reset_index(inplace=True, drop=False)

    category = color_stats['time_control_category'].head(1).item()
    results = color_stats[color_stats['time_control_category'] == category]
    first_results = results.head(1)
    first_color = first_results['player_color'].item()
    first_win_rate = first_results['Win'].item() * 100
    first_win_rate_str = (f'You had a {first_win_rate:.2f}% win rate'
                          f' with {first_color} in {category}')

    if color_stats.shape[0] > 1:
        if results.shape[0] > 1:
            results = results.tail(1)
            second_category_substr = ''
        else:
            # second row should be a different category
            results = color_stats.iloc[[1]]

            # results should be non-empty no matter what
            # since we have 2 rows at least, and either we have the same
            # category (we don't hit this line) or we have different
            # categories (this line is valid)
            second_category = results['time_control_category'].item()
            second_category_substr = ' in ' + second_category

        second_color = results['player_color'].item()
        second_win_rate = results['Win'].item() * 100

        second_win_rate_str = (f' and a {second_win_rate:.2f}% win rate '
                               f'with {second_color}'
                               f'{second_category_substr}')

    else:
        second_win_rate_str = ''

    win_rate_string = first_win_rate_str + second_win_rate_str + '.'

    return win_rate_string
