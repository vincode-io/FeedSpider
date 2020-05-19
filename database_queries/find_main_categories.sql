WITH RECURSIVE subcategories(id, name, artice_count, depth, path, cycle) AS (
		SELECT id, name, article_count, 0 depth, ARRAY[id], false
		FROM fs_category
		WHERE name = 'main topic classifications'
	UNION
		SELECT c.id, c.name, c.article_count, sc.depth + 1, path || c.id, c.id = ANY(path)
		FROM fs_category c
		INNER JOIN fs_category_relationship cr on c.id = cr.fs_category_child_id
		INNER JOIN subcategories sc ON sc.id = cr.fs_category_parent_id
		WHERE NOT cycle and sc.depth < 2
)
SELECT * FROM subcategories