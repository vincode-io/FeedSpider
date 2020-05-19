WITH RECURSIVE graph(id, name, artice_count, depth, path, cycle) AS (
		SELECT id, '', article_count, 0 depth, ARRAY[id], false
		FROM fs_category
		WHERE name = 'main topic classifications'
	UNION
		SELECT c.id, g.name || ' / ' || c.name, c.article_count, g.depth + 1, path || c.id, c.id = ANY(path)
		FROM fs_category c
		INNER JOIN fs_category_relationship cr on c.id = cr.fs_category_child_id
		INNER JOIN graph g ON g.id = cr.fs_category_parent_id
		WHERE NOT cycle and g.depth < 2
)
SELECT name FROM graph
WHERE depth > 1
order by name