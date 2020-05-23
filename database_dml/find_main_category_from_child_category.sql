WITH RECURSIVE graph(id, name, main_category, depth, path, cycle) AS (
		SELECT id, '', main_category, 0 depth, ARRAY[id], false
		FROM fs_category
		WHERE name = 'iata members'
	UNION
		SELECT c.id, c.name, c.main_category, g.depth + 1, path || c.id, c.id = ANY(path)
		FROM graph g
		INNER JOIN fs_category_relationship cr on g.id = cr.fs_category_child_id
		INNER JOIN fs_category c on cr.fs_category_parent_id = c.id
		WHERE NOT cycle and depth < 5
)
SELECT DISTINCT name FROM graph
where main_category = true
limit 5
