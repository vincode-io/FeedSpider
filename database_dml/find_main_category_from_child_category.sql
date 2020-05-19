WITH RECURSIVE graph(id, name, main_category, artice_count, depth, path, cycle) AS (
		SELECT id, '', main_category, article_count, 0 depth, ARRAY[id], false
		FROM fs_category
		WHERE name = 'films about drugs'
	UNION
		SELECT c.id, c.name, c.main_category, c.article_count, g.depth + 1, path || c.id, c.id = ANY(path)
		FROM fs_category c
		INNER JOIN fs_category_relationship cr on c.id = cr.fs_category_parent_id
		INNER JOIN graph g ON g.id = cr.fs_category_child_id
		WHERE NOT cycle and depth < 5
)
SELECT * FROM graph
where main_category = true
limit 5
